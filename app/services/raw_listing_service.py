"""
Raw Listing Service
===================

Business logic for raw file marketplace listing CRUD and lifecycle.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-05
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlmodel import select, func

from app.models.raw_listing import RawListing

logger = logging.getLogger(__name__)


def _get_db_session():
    from app.core.database import get_session_context
    return get_session_context()


class RawListingService:
    """Manages raw file marketplace listing lifecycle."""

    def create_listing(
        self,
        raw_file_id: str,
        title: str,
        description: str,
        tags: Optional[List[str]] = None,
        price_cents: Optional[int] = None,
    ) -> RawListing:
        listing = RawListing(
            id=str(uuid.uuid4()),
            raw_file_id=raw_file_id,
            title=title,
            description=description,
            tags=tags or [],
            price_cents=price_cents,
            status="draft",
        )
        with _get_db_session() as session:
            session.add(listing)
            session.commit()
            session.refresh(listing)
            logger.info("Created draft listing %s for file %s", listing.id, raw_file_id)
            return listing

    def update_listing(
        self,
        listing_id: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        price_cents: Optional[int] = None,
    ) -> Optional[RawListing]:
        with _get_db_session() as session:
            listing = session.exec(
                select(RawListing).where(RawListing.id == listing_id)
            ).first()
            if listing is None:
                return None

            if title is not None:
                listing.title = title
            if description is not None:
                listing.description = description
            if tags is not None:
                listing.tags = tags
            if price_cents is not None:
                listing.price_cents = price_cents

            listing.updated_at = datetime.now(timezone.utc)
            session.add(listing)
            session.commit()
            session.refresh(listing)
            return listing

    def list_listings(
        self,
        status_filter: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[RawListing], int]:
        with _get_db_session() as session:
            query = select(RawListing)
            count_query = select(func.count()).select_from(RawListing)

            if status_filter:
                query = query.where(RawListing.status == status_filter)
                count_query = count_query.where(RawListing.status == status_filter)

            total = session.exec(count_query).one()
            listings = list(
                session.exec(
                    query.order_by(RawListing.created_at.desc()).offset(offset).limit(limit)
                ).all()
            )
            return listings, total

    def get_listing_for_file(self, raw_file_id: str) -> Optional[RawListing]:
        """Get the most recent listing for a given raw file, if any."""
        with _get_db_session() as session:
            return session.exec(
                select(RawListing)
                .where(RawListing.raw_file_id == raw_file_id)
                .order_by(RawListing.created_at.desc())
            ).first()

    def get_listing(self, listing_id: str) -> Optional[RawListing]:
        with _get_db_session() as session:
            return session.exec(
                select(RawListing).where(RawListing.id == listing_id)
            ).first()

    def publish_listing(self, listing_id: str) -> RawListing:
        with _get_db_session() as session:
            listing = session.exec(
                select(RawListing).where(RawListing.id == listing_id)
            ).first()
            if listing is None:
                raise FileNotFoundError(f"Listing not found: {listing_id}")
            if listing.status == "listed":
                raise ValueError("Listing is already published")
            if listing.status == "delisted":
                raise ValueError("Cannot publish a delisted listing")

            listing.status = "listed"
            listing.published_at = datetime.now(timezone.utc)
            listing.updated_at = datetime.now(timezone.utc)
            session.add(listing)
            session.commit()
            session.refresh(listing)
            logger.info("Published raw listing %s", listing_id)
            return listing

    def delist_listing(self, listing_id: str) -> RawListing:
        with _get_db_session() as session:
            listing = session.exec(
                select(RawListing).where(RawListing.id == listing_id)
            ).first()
            if listing is None:
                raise FileNotFoundError(f"Listing not found: {listing_id}")
            if listing.status != "listed":
                raise ValueError("Only listed listings can be delisted")

            listing.status = "delisted"
            listing.updated_at = datetime.now(timezone.utc)
            session.add(listing)
            session.commit()
            session.refresh(listing)
            logger.info("Delisted raw listing %s", listing_id)
            return listing


_raw_listing_service: Optional[RawListingService] = None


def get_raw_listing_service() -> RawListingService:
    global _raw_listing_service
    if _raw_listing_service is None:
        _raw_listing_service = RawListingService()
    return _raw_listing_service
