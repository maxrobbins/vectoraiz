"""
API Documentation endpoints.
Provides usage guides, examples, and exportable collections.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/guide", summary="API Usage Guide", description="Comprehensive guide for using the vectorAIz API.")
async def api_guide():
    """
    Returns a comprehensive guide for using the vectorAIz API.
    """
    return {
        "title": "vectorAIz API Usage Guide",
        "version": "1.0.0",
        "sections": [
            {
                "title": "1. Getting Started",
                "content": """
                    vectorAIz processes your data files and enables semantic search.
                    
                    Basic workflow:
                    1. Upload a file (CSV, JSON, Parquet, PDF, Word, Excel)
                    2. Wait for processing (automatic)
                    3. Search using natural language or SQL
                """,
                "example": {
                    "upload": "curl -X POST http://localhost:8000/api/datasets/upload -F 'file=@data.csv'",
                    "search": "curl 'http://localhost:8000/api/search?q=technology%20companies'",
                }
            },
            {
                "title": "2. Dataset Management",
                "endpoints": [
                    {"method": "POST", "path": "/api/datasets/upload", "description": "Upload a new dataset"},
                    {"method": "GET", "path": "/api/datasets", "description": "List all datasets"},
                    {"method": "GET", "path": "/api/datasets/{id}", "description": "Get dataset details"},
                    {"method": "GET", "path": "/api/datasets/{id}/sample", "description": "Get sample rows"},
                    {"method": "GET", "path": "/api/datasets/{id}/statistics", "description": "Get column statistics"},
                    {"method": "GET", "path": "/api/datasets/{id}/profile", "description": "Get column profiles"},
                    {"method": "GET", "path": "/api/datasets/{id}/full", "description": "Get complete metadata"},
                    {"method": "DELETE", "path": "/api/datasets/{id}", "description": "Delete a dataset"},
                ],
            },
            {
                "title": "3. Semantic Search",
                "content": """
                    Search uses natural language understanding to find relevant data.
                    Results are ranked by semantic similarity to your query.
                """,
                "endpoints": [
                    {"method": "GET", "path": "/api/search?q={query}", "description": "Search across all datasets"},
                    {"method": "GET", "path": "/api/search/dataset/{id}?q={query}", "description": "Search within a dataset"},
                    {"method": "GET", "path": "/api/search/suggest?q={partial}", "description": "Get search suggestions"},
                    {"method": "GET", "path": "/api/search/stats", "description": "Get search statistics"},
                ],
                "example": {
                    "basic": "curl 'http://localhost:8000/api/search?q=revenue%20growth'",
                    "filtered": "curl 'http://localhost:8000/api/search?q=technology&limit=5&min_score=0.5'",
                }
            },
            {
                "title": "4. SQL Queries",
                "content": """
                    Execute SQL SELECT queries directly against your datasets.
                    Datasets are exposed as tables named 'dataset_{id}'.
                """,
                "endpoints": [
                    {"method": "GET", "path": "/api/sql/tables", "description": "List available tables"},
                    {"method": "GET", "path": "/api/sql/tables/{id}", "description": "Get table schema"},
                    {"method": "POST", "path": "/api/sql/query", "description": "Execute SQL query"},
                    {"method": "POST", "path": "/api/sql/validate", "description": "Validate query syntax"},
                ],
                "example": {
                    "list_tables": "curl http://localhost:8000/api/sql/tables",
                    "query": """curl -X POST http://localhost:8000/api/sql/query -H 'Content-Type: application/json' -d '{"query": "SELECT * FROM dataset_abc123 LIMIT 10"}'""",
                }
            },
            {
                "title": "5. PII Detection",
                "content": """
                    Automatically scans datasets for personally identifiable information.
                    Detects emails, phone numbers, SSNs, credit cards, and more.
                """,
                "endpoints": [
                    {"method": "GET", "path": "/api/pii/entities", "description": "List detectable PII types"},
                    {"method": "GET", "path": "/api/pii/scan/{id}", "description": "Get cached PII scan"},
                    {"method": "POST", "path": "/api/pii/scan/{id}", "description": "Trigger new PII scan"},
                    {"method": "POST", "path": "/api/pii/analyze-text", "description": "Analyze text for PII"},
                ],
            },
            {
                "title": "6. Vector Management",
                "content": """
                    Manage the underlying vector database collections.
                    Each dataset creates a collection named 'dataset_{id}'.
                """,
                "endpoints": [
                    {"method": "GET", "path": "/api/vectors/health", "description": "Check Qdrant health"},
                    {"method": "GET", "path": "/api/vectors/collections", "description": "List all collections"},
                    {"method": "GET", "path": "/api/vectors/collections/{name}", "description": "Get collection info"},
                    {"method": "POST", "path": "/api/vectors/collections/{name}", "description": "Create collection"},
                    {"method": "DELETE", "path": "/api/vectors/collections/{name}", "description": "Delete collection"},
                ],
            },
        ],
        "supported_formats": {
            "tabular": ["CSV", "JSON", "Parquet"],
            "documents": ["PDF", "DOCX", "DOC", "PPTX", "PPT"],
            "spreadsheets": ["XLSX", "XLS"],
        },
        "limits": {
            "max_file_size": "100GB",
            "max_sql_rows": 10000,
            "default_search_limit": 10,
            "pii_sample_size": 1000,
        },
    }


@router.get("/postman", summary="Postman Collection", description="Export API as Postman collection for testing.")
async def postman_collection(request: Request):
    """
    Returns a Postman collection for importing into Postman.
    """
    base_url = str(request.base_url).rstrip("/")
    
    collection = {
        "info": {
            "name": "vectorAIz API",
            "description": "Data processing and semantic search API",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        "variable": [
            {"key": "base_url", "value": base_url, "type": "string"},
            {"key": "dataset_id", "value": "", "type": "string"},
        ],
        "item": [
            {
                "name": "Health",
                "item": [
                    {
                        "name": "Health Check",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/health",
                        },
                    },
                    {
                        "name": "Readiness Check",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/health/ready",
                        },
                    },
                ],
            },
            {
                "name": "Datasets",
                "item": [
                    {
                        "name": "List Datasets",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/datasets",
                        },
                    },
                    {
                        "name": "Upload Dataset",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/datasets/upload",
                            "body": {
                                "mode": "formdata",
                                "formdata": [
                                    {"key": "file", "type": "file", "src": ""}
                                ],
                            },
                        },
                    },
                    {
                        "name": "Get Dataset",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/datasets/{{dataset_id}}",
                        },
                    },
                    {
                        "name": "Get Sample Rows",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/datasets/{{dataset_id}}/sample?limit=10",
                        },
                    },
                    {
                        "name": "Get Full Metadata",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/datasets/{{dataset_id}}/full",
                        },
                    },
                    {
                        "name": "Delete Dataset",
                        "request": {
                            "method": "DELETE",
                            "url": "{{base_url}}/api/datasets/{{dataset_id}}",
                        },
                    },
                ],
            },
            {
                "name": "Search",
                "item": [
                    {
                        "name": "Search All",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/search?q=technology",
                        },
                    },
                    {
                        "name": "Search Dataset",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/search/dataset/{{dataset_id}}?q=revenue",
                        },
                    },
                    {
                        "name": "Search Stats",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/search/stats",
                        },
                    },
                    {
                        "name": "Search Suggestions",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/search/suggest?q=tech",
                        },
                    },
                ],
            },
            {
                "name": "SQL",
                "item": [
                    {
                        "name": "List Tables",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/sql/tables",
                        },
                    },
                    {
                        "name": "Get Table Schema",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/sql/tables/{{dataset_id}}",
                        },
                    },
                    {
                        "name": "Execute Query",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/sql/query",
                            "header": [
                                {"key": "Content-Type", "value": "application/json"}
                            ],
                            "body": {
                                "mode": "raw",
                                "raw": '{"query": "SELECT * FROM dataset_{{dataset_id}} LIMIT 10"}',
                            },
                        },
                    },
                    {
                        "name": "Validate Query",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/sql/validate",
                            "header": [
                                {"key": "Content-Type", "value": "application/json"}
                            ],
                            "body": {
                                "mode": "raw",
                                "raw": '{"query": "SELECT * FROM test"}',
                            },
                        },
                    },
                    {
                        "name": "SQL Help",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/sql/help",
                        },
                    },
                ],
            },
            {
                "name": "PII",
                "item": [
                    {
                        "name": "List Entities",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/pii/entities",
                        },
                    },
                    {
                        "name": "Scan Dataset",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/pii/scan/{{dataset_id}}",
                        },
                    },
                    {
                        "name": "Get Scan Results",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/pii/scan/{{dataset_id}}",
                        },
                    },
                    {
                        "name": "Analyze Text",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/pii/analyze-text?text=Email%20me%20at%20test@example.com",
                        },
                    },
                ],
            },
            {
                "name": "Vectors",
                "item": [
                    {
                        "name": "Vector Health",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/vectors/health",
                        },
                    },
                    {
                        "name": "List Collections",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/vectors/collections",
                        },
                    },
                    {
                        "name": "Embedding Info",
                        "request": {
                            "method": "GET",
                            "url": "{{base_url}}/api/vectors/embedding/info",
                        },
                    },
                    {
                        "name": "Test Embedding",
                        "request": {
                            "method": "POST",
                            "url": "{{base_url}}/api/vectors/embedding/test?text=Hello%20world",
                        },
                    },
                ],
            },
        ],
    }
    
    return JSONResponse(
        content=collection,
        headers={
            "Content-Disposition": "attachment; filename=vectoraiz-api.postman_collection.json"
        }
    )


@router.get("/examples", summary="Request Examples", description="Example requests and responses for all endpoints.")
async def api_examples():
    """
    Returns example requests and responses for key endpoints.
    """
    return {
        "examples": [
            {
                "name": "Upload CSV File",
                "endpoint": "POST /api/datasets/upload",
                "curl": "curl -X POST http://localhost:8000/api/datasets/upload -F 'file=@companies.csv'",
                "response": {
                    "message": "File uploaded successfully. Processing started.",
                    "dataset_id": "abc12345",
                    "status": "uploading",
                    "filename": "companies.csv"
                }
            },
            {
                "name": "Semantic Search",
                "endpoint": "GET /api/search?q=technology",
                "curl": "curl 'http://localhost:8000/api/search?q=technology%20companies&limit=5'",
                "response": {
                    "query": "technology companies",
                    "results": [
                        {
                            "dataset_id": "abc12345",
                            "dataset_name": "companies.csv",
                            "score": 0.8542,
                            "row_index": 3,
                            "text_content": "company_name: TechCorp | industry: Technology",
                            "row_data": {"id": 4, "company_name": "TechCorp", "industry": "Technology"}
                        }
                    ],
                    "total": 1,
                    "duration_ms": 45.2
                }
            },
            {
                "name": "SQL Query",
                "endpoint": "POST /api/sql/query",
                "curl": """curl -X POST http://localhost:8000/api/sql/query -H 'Content-Type: application/json' -d '{"query": "SELECT company_name, revenue FROM dataset_abc12345 WHERE revenue > 1000000"}'""",
                "response": {
                    "query": "SELECT company_name, revenue FROM dataset_abc12345 WHERE revenue > 1000000",
                    "columns": ["company_name", "revenue"],
                    "data": [
                        {"company_name": "Acme Corp", "revenue": 1500000},
                        {"company_name": "TechCorp", "revenue": 2300000}
                    ],
                    "row_count": 2,
                    "duration_ms": 12.5
                }
            },
            {
                "name": "PII Scan",
                "endpoint": "POST /api/pii/scan/{dataset_id}",
                "curl": "curl -X POST http://localhost:8000/api/pii/scan/abc12345",
                "response": {
                    "dataset_id": "abc12345",
                    "overall_risk": "medium",
                    "columns_with_pii": 2,
                    "pii_findings": [
                        {
                            "column_name": "email",
                            "pii_detected": True,
                            "entity_types": {"EMAIL_ADDRESS": 5},
                            "risk_level": "medium"
                        }
                    ],
                    "recommendations": [
                        {
                            "severity": "medium",
                            "message": "Personal identifiers found that may require anonymization."
                        }
                    ]
                }
            },
        ],
        "error_responses": {
            "400": {"detail": "Invalid request or query validation failed"},
            "404": {"detail": "Resource not found"},
            "500": {"detail": "Internal server error"}
        }
    }
