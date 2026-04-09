"""
PII Detection service using Microsoft Presidio.
Scans datasets for personally identifiable information.

PRIVACY SCORE AUTHORITY: This is the authoritative source for privacy_score.
The score is calculated here and validated by ai.market central service.
Frontend should display this score, not recalculate it.

Updated: January 19, 2026 - Added privacy_score calculation
Updated: February 7, 2026 - Added per-column PII config persistence (BQ-065)
"""

from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import json
import logging

from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

from app.config import settings
from app.services.duckdb_service import ephemeral_duckdb_service

# PII settings file for configurable thresholds / entity overrides
_PII_SETTINGS_FILE = "pii_settings.json"

logger = logging.getLogger(__name__)

# PII entity types to detect
DEFAULT_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS", 
    "PHONE_NUMBER",
    "US_SSN",
    "CREDIT_CARD",
    "US_PASSPORT",
    "US_DRIVER_LICENSE",
    "IP_ADDRESS",
    "IBAN_CODE",
    "US_BANK_NUMBER",
    "LOCATION",
    "DATE_TIME",
    "NRP",  # Nationality, Religion, Political group
    "MEDICAL_LICENSE",
    "URL",
]

# Sample size for PII scanning (balance between accuracy and speed)
DEFAULT_SAMPLE_SIZE = 1000

# Minimum confidence threshold
DEFAULT_SCORE_THRESHOLD = 0.5

# Valid per-column PII actions
VALID_PII_ACTIONS = {"exclude", "redact", "keep"}


class PIIResult:
    """Represents PII detection results for a column."""
    
    def __init__(self, column_name: str):
        self.column_name = column_name
        self.pii_detected = False
        self.entity_types: Dict[str, int] = {}  # entity_type -> count
        self.max_confidence: float = 0.0
        self.sample_matches: List[Dict[str, Any]] = []
    
    def add_match(self, entity_type: str, confidence: float, sample_value: str):
        """Add a PII match."""
        self.pii_detected = True
        self.entity_types[entity_type] = self.entity_types.get(entity_type, 0) + 1
        self.max_confidence = max(self.max_confidence, confidence)
        
        # Store sample (limit to 3 per column)
        if len(self.sample_matches) < 3:
            # Mask the actual value for safety
            masked = self._mask_value(sample_value)
            self.sample_matches.append({
                "entity_type": entity_type,
                "confidence": round(confidence, 3),
                "masked_value": masked,
            })
    
    def _mask_value(self, value: str) -> str:
        """Mask PII value for safe display."""
        if len(value) <= 4:
            return "****"
        return value[:2] + "*" * (len(value) - 4) + value[-2:]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column_name,
            "pii_detected": self.pii_detected,
            "pii_types": list(self.entity_types.keys()),
            "entity_counts": self.entity_types,
            "max_confidence": round(self.max_confidence, 3),
            "sample_matches": self.sample_matches,
            "risk_level": self._calculate_risk_level(),
        }
    
    def _calculate_risk_level(self) -> str:
        """Calculate risk level based on PII types and confidence."""
        if not self.pii_detected:
            return "none"
        
        high_risk_types = {"US_SSN", "CREDIT_CARD", "US_PASSPORT", "US_DRIVER_LICENSE", "US_BANK_NUMBER"}
        medium_risk_types = {"PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "IBAN_CODE", "LOCATION"}
        
        for entity_type in self.entity_types:
            if entity_type in high_risk_types and self.max_confidence >= 0.7:
                return "high"
        
        for entity_type in self.entity_types:
            if entity_type in medium_risk_types and self.max_confidence >= 0.6:
                return "medium"
        
        return "low"


class PIIService:
    """
    Detects PII in datasets using Microsoft Presidio.
    """
    
    def __init__(self):
        self._analyzer: Optional[AnalyzerEngine] = None
        self._config_dir = Path(settings.data_directory) / "pii_configs"
        self._config_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def analyzer(self) -> AnalyzerEngine:
        """Lazy load the Presidio analyzer."""
        if self._analyzer is None:
            try:
                self._analyzer = self._create_analyzer()
            except Exception as e:
                raise OSError(
                    f"Failed to initialize PII analyzer: {e}. "
                    "Ensure spaCy model is installed: python -m spacy download en_core_web_sm"
                ) from e
        return self._analyzer
    
    def _create_analyzer(self) -> AnalyzerEngine:
        """Create and configure the Presidio analyzer engine."""
        # Use spaCy NLP engine
        configuration = {
            "nlp_engine_name": "spacy",
            "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}]
        }
        
        provider = NlpEngineProvider(nlp_configuration=configuration)
        nlp_engine = provider.create_engine()
        
        analyzer = AnalyzerEngine(
            nlp_engine=nlp_engine,
            supported_languages=["en"],
        )
        
        return analyzer

    # ── Per-column PII config persistence (BQ-065) ──────────────────────

    def _config_path(self, dataset_id: str) -> Path:
        """Return the JSON file path for a dataset's PII column config."""
        return self._config_dir / f"{dataset_id}.json"

    def save_pii_config(
        self,
        dataset_id: str,
        column_actions: Dict[str, str],
    ) -> Dict[str, Any]:
        """
        Persist per-column PII action decisions for a dataset.

        Args:
            dataset_id: The dataset identifier.
            column_actions: Mapping of column_name -> action.
                Valid actions: "exclude", "redact", "keep".

        Returns:
            Saved config dict with metadata.

        Raises:
            ValueError: If any action value is invalid.
        """
        # Validate actions
        invalid = {
            col: action
            for col, action in column_actions.items()
            if action not in VALID_PII_ACTIONS
        }
        if invalid:
            raise ValueError(
                f"Invalid action(s): {invalid}. "
                f"Valid actions are: {', '.join(sorted(VALID_PII_ACTIONS))}"
            )

        config = {
            "dataset_id": dataset_id,
            "column_actions": column_actions,
            "updated_at": datetime.utcnow().isoformat(),
        }

        config_path = self._config_path(dataset_id)
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        logger.info(
            "Saved PII config for dataset %s: %d columns configured",
            dataset_id,
            len(column_actions),
        )
        return config

    def get_pii_config(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve saved per-column PII action decisions for a dataset.

        Args:
            dataset_id: The dataset identifier.

        Returns:
            Config dict with column_actions and metadata, or None if not found.
        """
        config_path = self._config_path(dataset_id)
        if not config_path.exists():
            return None

        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read PII config for %s: %s", dataset_id, e)
            return None

    # ── Scanning ────────────────────────────────────────────────────────
    
    def scan_text(
        self, 
        text: str, 
        entities: Optional[List[str]] = None,
        score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    ) -> List[RecognizerResult]:
        """
        Scan a single text string for PII.
        
        Args:
            text: Text to scan
            entities: Entity types to detect (None = all)
            score_threshold: Minimum confidence score
            
        Returns:
            List of Presidio RecognizerResult objects
        """
        if not text or not isinstance(text, str):
            return []
        
        entities = entities or DEFAULT_ENTITIES
        
        results = self.analyzer.analyze(
            text=text,
            entities=entities,
            language="en",
            score_threshold=score_threshold,
        )
        
        return results
    
    def _calculate_privacy_score(
        self, 
        pii_findings: List[Dict[str, Any]], 
        overall_risk: str
    ) -> float:
        """
        Calculate privacy score on a 0-10 scale.
        
        AUTHORITATIVE SCORE: This is the source of truth for privacy scoring.
        ai.market central service validates this score but vectorAIz calculates it.
        
        Scoring logic:
        - Start at 10.0 (perfect privacy)
        - Deduct based on PII findings and risk levels
        - Minimum score is 0.0
        
        Args:
            pii_findings: List of column results with PII
            overall_risk: Overall risk level (none, low, medium, high)
            
        Returns:
            Privacy score from 0.0 to 10.0
        """
        if not pii_findings or overall_risk == "none":
            return 10.0
        
        score = 10.0
        
        for finding in pii_findings:
            risk_level = finding.get("risk_level", "low")
            
            if risk_level == "high":
                score -= 3.0
            elif risk_level == "medium":
                score -= 2.0
            elif risk_level == "low":
                score -= 1.0
        
        # Ensure score doesn't go below 0
        return max(0.0, round(score, 1))
    
    # ── Global PII settings (BQ-VZ-DATA-READINESS) ─────────────────────

    def _settings_path(self) -> Path:
        return Path(settings.data_directory) / _PII_SETTINGS_FILE

    def get_pii_settings(self) -> Dict[str, Any]:
        """Get global PII settings (score threshold, entity overrides, excluded patterns)."""
        path = self._settings_path()
        if not path.exists():
            return {
                "score_threshold": DEFAULT_SCORE_THRESHOLD,
                "entity_overrides": {},
                "excluded_patterns": [],
            }
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {
                "score_threshold": DEFAULT_SCORE_THRESHOLD,
                "entity_overrides": {},
                "excluded_patterns": [],
            }

    def save_pii_settings(self, pii_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Save global PII settings."""
        # Validate threshold
        threshold = pii_settings.get("score_threshold", DEFAULT_SCORE_THRESHOLD)
        if not (0.0 <= threshold <= 1.0):
            raise ValueError("score_threshold must be between 0.0 and 1.0")

        config = {
            "score_threshold": threshold,
            "entity_overrides": pii_settings.get("entity_overrides", {}),
            "excluded_patterns": pii_settings.get("excluded_patterns", []),
            "updated_at": datetime.utcnow().isoformat(),
        }
        path = self._settings_path()
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        logger.info("Saved global PII settings: threshold=%.2f", threshold)
        return config

    # ── Structured scanning (BQ-VZ-DATA-READINESS) ───────────────────

    def scan_structured(
        self,
        filepath: Path,
        sample_size: int = DEFAULT_SAMPLE_SIZE,
    ) -> Dict[str, Any]:
        """Column-aware structured PII scan.

        Uses the Presidio analyzer on each column with type-aware heuristics.
        Applies configurable score thresholds and excluded patterns from settings.
        Sensor IDs and similar domain-specific patterns are excluded by default.
        """
        pii_settings = self.get_pii_settings()
        threshold = pii_settings.get("score_threshold", DEFAULT_SCORE_THRESHOLD)
        excluded = set(pii_settings.get("excluded_patterns", []))
        entity_overrides = pii_settings.get("entity_overrides", {})

        # Determine which entities to scan per column
        entities = list(DEFAULT_ENTITIES)

        start_time = datetime.utcnow()

        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if file_type is None:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")
            read_func = duckdb.get_read_function(file_type, str(filepath))

            schema = duckdb.connection.execute(
                f"DESCRIBE SELECT * FROM {read_func}"
            ).fetchall()
            columns = [(row[0], row[1]) for row in schema]

            count_result = duckdb.connection.execute(
                f"SELECT COUNT(*) FROM {read_func}"
            ).fetchone()
            total_rows = count_result[0] if count_result else 0

            sample_query = (
                f"SELECT * FROM {read_func} "
                f"USING SAMPLE {min(sample_size, total_rows)}"
            )
            sample_rows = duckdb.connection.execute(sample_query).fetchall()

        column_names = [c[0] for c in columns]
        column_types = {c[0]: c[1].lower() for c in columns}
        column_results: Dict[str, PIIResult] = {col: PIIResult(col) for col in column_names}

        for row in sample_rows:
            for col_name, value in zip(column_names, row):
                if value is None:
                    continue
                text = str(value)
                if not text or len(text) >= 10000:
                    continue

                # Skip excluded patterns
                if any(pat in text for pat in excluded):
                    continue

                # Skip numeric-only columns for name/location detection
                col_type = column_types.get(col_name, "")
                scan_entities = list(entities)
                if any(t in col_type for t in ("int", "float", "double", "decimal")):
                    # Numeric columns: only scan for SSN, credit card, phone
                    scan_entities = [
                        e for e in entities
                        if e in ("US_SSN", "CREDIT_CARD", "PHONE_NUMBER", "IP_ADDRESS")
                    ]

                # Apply entity overrides (disable specific entities per column)
                col_overrides = entity_overrides.get(col_name, {})
                if col_overrides.get("disabled"):
                    continue
                disabled_entities = set(col_overrides.get("disabled_entities", []))
                scan_entities = [e for e in scan_entities if e not in disabled_entities]

                pii_matches = self.scan_text(text, scan_entities, threshold)
                for match in pii_matches:
                    column_results[col_name].add_match(
                        entity_type=match.entity_type,
                        confidence=match.score,
                        sample_value=text[match.start:match.end],
                    )

        columns_with_pii = [r.to_dict() for r in column_results.values() if r.pii_detected]
        columns_clean = [col for col in column_names if not column_results[col].pii_detected]

        overall_risk = "none"
        for result in column_results.values():
            risk = result._calculate_risk_level()
            if risk == "high":
                overall_risk = "high"
                break
            elif risk == "medium" and overall_risk != "high":
                overall_risk = "medium"
            elif risk == "low" and overall_risk == "none":
                overall_risk = "low"

        privacy_score = self._calculate_privacy_score(columns_with_pii, overall_risk)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        return {
            "scanned_at": start_time.isoformat(),
            "scan_type": "structured",
            "total_rows": total_rows,
            "rows_sampled": len(sample_rows),
            "total_columns": len(column_names),
            "columns_with_pii": len(columns_with_pii),
            "columns_clean": len(columns_clean),
            "overall_risk": overall_risk,
            "privacy_score": privacy_score,
            "column_results": columns_with_pii,
            "clean_columns": columns_clean,
            "duration_seconds": round(duration, 2),
            "entities_checked": entities,
            "score_threshold": threshold,
        }

    def scan_text_content(
        self,
        text_blocks: List[str],
        sample_size: int = 50,
        entities: Optional[List[str]] = None,
        score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    ) -> Dict[str, Any]:
        """Scan extracted text content (from documents) for PII.

        Unlike scan_dataset() which reads tabular files via DuckDB,
        this scans raw text blocks from document extraction (Tika, etc.).

        Args:
            text_blocks: List of text strings (extracted document content)
            sample_size: Max number of blocks to sample
            entities: Entity types to detect (None = all)
            score_threshold: Minimum confidence score

        Returns:
            PII scan results in the same format as scan_dataset(),
            using "document_content" as a pseudo-column name.
        """
        import random

        start_time = datetime.utcnow()
        entities = entities or DEFAULT_ENTITIES
        total_blocks = len(text_blocks)

        # Sample blocks if there are too many
        if total_blocks > sample_size:
            sampled = random.sample(text_blocks, sample_size)
        else:
            sampled = list(text_blocks)

        # Use a single PIIResult to aggregate all findings under a pseudo-column
        result = PIIResult("document_content")

        for block in sampled:
            if not block or not isinstance(block, str):
                continue
            # Truncate very long blocks to avoid slow NLP processing
            text = block[:10000] if len(block) > 10000 else block
            if not text.strip():
                continue

            pii_matches = self.scan_text(text, entities, score_threshold)
            for match in pii_matches:
                result.add_match(
                    entity_type=match.entity_type,
                    confidence=match.score,
                    sample_value=text[match.start:match.end],
                )

        # Build column_results in same format as scan_dataset
        columns_with_pii = [result.to_dict()] if result.pii_detected else []

        # Calculate overall risk
        overall_risk = result._calculate_risk_level()

        # Calculate privacy score (AUTHORITATIVE)
        privacy_score = self._calculate_privacy_score(columns_with_pii, overall_risk)

        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        return {
            "scanned_at": start_time.isoformat(),
            "total_rows": total_blocks,
            "rows_sampled": len(sampled),
            "total_columns": 1,
            "columns_with_pii": len(columns_with_pii),
            "columns_clean": 0 if result.pii_detected else 1,
            "overall_risk": overall_risk,
            "privacy_score": privacy_score,
            "column_results": columns_with_pii,
            "clean_columns": [] if result.pii_detected else ["document_content"],
            "duration_seconds": round(duration_seconds, 2),
            "entities_checked": entities,
            "scan_type": "text_content",
            "total_blocks": total_blocks,
            "blocks_sampled": len(sampled),
        }

    def scan_dataset(
        self,
        filepath: Path,
        sample_size: int = DEFAULT_SAMPLE_SIZE,
        entities: Optional[List[str]] = None,
        score_threshold: float = DEFAULT_SCORE_THRESHOLD,
    ) -> Dict[str, Any]:
        """
        Scan a dataset for PII.
        
        Args:
            filepath: Path to the Parquet file
            sample_size: Number of rows to sample
            entities: Entity types to detect
            score_threshold: Minimum confidence
            
        Returns:
            PII scan results with column-level analysis and privacy_score
        """
        start_time = datetime.utcnow()
        entities = entities or DEFAULT_ENTITIES
        
        # Get dataset metadata using ephemeral connection (thread-safe)
        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if file_type is None:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")
            read_func = duckdb.get_read_function(file_type, str(filepath))

            # Get column names
            schema = duckdb.connection.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()
            columns = [row[0] for row in schema]

            # Get total row count
            count_result = duckdb.connection.execute(f"SELECT COUNT(*) FROM {read_func}").fetchone()
            total_rows = count_result[0] if count_result else 0

            # Sample rows for scanning
            sample_query = f"SELECT * FROM {read_func} USING SAMPLE {min(sample_size, total_rows)}"
            sample_rows = duckdb.connection.execute(sample_query).fetchall()
        
        # Initialize results for each column
        column_results: Dict[str, PIIResult] = {col: PIIResult(col) for col in columns}
        
        # Scan each cell
        for row in sample_rows:
            for col_name, value in zip(columns, row):
                if value is not None:
                    text = str(value)
                    if len(text) > 0 and len(text) < 10000:  # Skip empty and very long texts
                        pii_matches = self.scan_text(text, entities, score_threshold)
                        
                        for match in pii_matches:
                            column_results[col_name].add_match(
                                entity_type=match.entity_type,
                                confidence=match.score,
                                sample_value=text[match.start:match.end],
                            )
        
        # Compile results
        columns_with_pii = [r.to_dict() for r in column_results.values() if r.pii_detected]
        columns_clean = [col for col in columns if not column_results[col].pii_detected]
        
        # Calculate overall risk
        overall_risk = "none"
        for result in column_results.values():
            risk = result._calculate_risk_level()
            if risk == "high":
                overall_risk = "high"
                break
            elif risk == "medium" and overall_risk != "high":
                overall_risk = "medium"
            elif risk == "low" and overall_risk == "none":
                overall_risk = "low"
        
        # Calculate privacy score (AUTHORITATIVE)
        privacy_score = self._calculate_privacy_score(columns_with_pii, overall_risk)
        
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()
        
        return {
            "scanned_at": start_time.isoformat(),
            "total_rows": total_rows,
            "rows_sampled": len(sample_rows),
            "total_columns": len(columns),
            "columns_with_pii": len(columns_with_pii),
            "columns_clean": len(columns_clean),
            "overall_risk": overall_risk,
            "privacy_score": privacy_score,
            "column_results": columns_with_pii,
            "clean_columns": columns_clean,
            "duration_seconds": round(duration_seconds, 2),
            "entities_checked": entities,
        }

    def scrub_dataset(
        self,
        filepath: Path,
        strategy: str = "mask",
        sample_size: int = DEFAULT_SAMPLE_SIZE,
    ) -> Dict[str, Any]:
        """Scrub a dataset for PII using Presidio Anonymizer.

        Args:
            filepath: Path to the Parquet file
            strategy: "mask", "redact", or "hash"
            sample_size: Number of rows to sample for initial scan

        Returns:
            Dict with scrubbed file info, PII counts, and privacy score.
        """
        start_time = datetime.utcnow()

        # 1. Scan the original dataset
        before_scan = self.scan_dataset(filepath, sample_size)

        # 2. Read the full dataset into rows using ephemeral connection (thread-safe)
        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if file_type is None:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")
            read_func = duckdb.get_read_function(file_type, str(filepath))
            rows = duckdb.connection.execute(f"SELECT * FROM {read_func}").fetchall()
            schema = duckdb.connection.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()
            columns = [row[0] for row in schema]

        # 3. Determine the anonymization operator
        if strategy == "mask":
            operator = OperatorConfig("replace", {"new_value": "***"})
        elif strategy == "redact":
            operator = OperatorConfig("redact", {})
        elif strategy == "hash":
            operator = OperatorConfig("hash", {"hash_type": "sha256"})
        else:
            raise ValueError(f"Invalid strategy: {strategy}")

        # 4. Apply the chosen strategy to cells containing PII
        anonymizer = AnonymizerEngine()
        scrubbed_rows = []
        pii_removed_count = 0

        for row in rows:
            scrubbed_row = list(row)
            for i, col_name in enumerate(columns):
                value = row[i]
                if value is not None:
                    text = str(value)
                    # Check if this column had PII
                    column_result = next((r for r in before_scan["column_results"] if r["column"] == col_name), None)
                    if column_result:
                        pii_matches = self.scan_text(text)
                        if pii_matches:
                            # Anonymize the text
                            anonymized_results = anonymizer.anonymize(
                                text=text,
                                analyzer_results=pii_matches,
                                operators={match.entity_type: operator for match in pii_matches}
                            )
                            scrubbed_row[i] = anonymized_results.text
                            pii_removed_count += 1 # rough count

            scrubbed_rows.append(tuple(scrubbed_row))

        # 5. Write the scrubbed data to a new Parquet file
        original_filepath = Path(filepath)
        scrubbed_filepath = original_filepath.with_name(original_filepath.stem + "_scrubbed" + original_filepath.suffix)
        with ephemeral_duckdb_service() as duckdb:
            duckdb.write_parquet(scrubbed_filepath, scrubbed_rows, columns)

        # 6. Scan the scrubbed dataset
        after_scan = self.scan_dataset(scrubbed_filepath, sample_size)

        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        # 7. Return the results
        return {
            "scrubbed_filepath": str(scrubbed_filepath),
            "strategy_used": strategy,
            "before_scan": before_scan,
            "after_scan": after_scan,
            "pii_removed_count": pii_removed_count,
            "duration_seconds": round(duration_seconds, 2),
        }
    
    def get_recommendations(self, scan_result: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Generate recommendations based on PII scan results.
        """
        recommendations = []
        
        if scan_result["overall_risk"] == "high":
            recommendations.append({
                "severity": "high",
                "message": "High-risk PII detected. Consider excluding these columns before publishing.",
                "action": "Review columns with SSN, credit card, or passport data.",
            })
        
        if scan_result["overall_risk"] in ["high", "medium"]:
            recommendations.append({
                "severity": "medium",
                "message": "Personal identifiers found that may require anonymization.",
                "action": "Consider masking or removing personal names, emails, and phone numbers.",
            })
        
        # Use column_results instead of pii_findings for consistency
        pii_findings = scan_result.get("column_results", scan_result.get("pii_findings", []))
        for finding in pii_findings:
            if finding["risk_level"] == "high":
                col_name = finding.get("column", finding.get("column_name", "unknown"))
                pii_types = finding.get("pii_types", list(finding.get("entity_types", {}).keys()))
                recommendations.append({
                    "severity": "high",
                    "message": f"Column '{col_name}' contains {', '.join(pii_types)}",
                    "action": f"Exclude or anonymize column '{col_name}' before publishing.",
                })
        
        if not recommendations:
            recommendations.append({
                "severity": "info",
                "message": "No significant PII detected in sampled data.",
                "action": "Dataset appears safe for publishing, but review manually if sensitive.",
            })
        
        return recommendations


# Singleton instance
_pii_service: Optional[PIIService] = None


def get_pii_service() -> PIIService:
    """Get the singleton PII service instance."""
    global _pii_service
    if _pii_service is None:
        _pii_service = PIIService()
    return _pii_service
