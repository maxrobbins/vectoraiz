"""
BQ-VZ-MULTI-USER: CLI Password Reset Tool
===========================================

Reset a user's password from the command line.

Usage::

    # Inside Docker container:
    python -m app.cli.reset_password --username admin --new-password <password>

    # Interactive (prompts for password):
    python -m app.cli.reset_password --username admin

    # Via docker exec:
    docker exec vectoraiz-backend python -m app.cli.reset_password --username admin

Phase: BQ-VZ-MULTI-USER — Admin/User Role Split
Created: 2026-03-03
"""

import argparse
import getpass
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Reset a vectorAIz user's password",
    )
    parser.add_argument(
        "--username",
        required=True,
        help="Username of the account to reset",
    )
    parser.add_argument(
        "--new-password",
        default=None,
        help="New password (prompted interactively if not provided)",
    )
    args = parser.parse_args()

    # Get password interactively if not provided
    new_password = args.new_password
    if not new_password:
        new_password = getpass.getpass("Enter new password: ")
        confirm = getpass.getpass("Confirm new password: ")
        if new_password != confirm:
            print("Error: passwords do not match.", file=sys.stderr)
            sys.exit(1)

    if len(new_password) < 8:
        print("Error: password must be at least 8 characters.", file=sys.stderr)
        sys.exit(1)

    # Import DB and models after arg parsing (avoid slow imports on --help)
    from app.core.database import init_db, get_session_context
    from app.models.user import User
    from app.services.auth_service import hash_password
    from sqlmodel import select

    # Initialize database (runs migrations)
    init_db()

    username = args.username.strip().lower()

    with get_session_context() as session:
        user = session.exec(
            select(User).where(User.username == username)
        ).first()

        if not user:
            print(f"Error: user '{username}' not found.", file=sys.stderr)
            sys.exit(1)

        user.pw_hash = hash_password(new_password)
        session.add(user)
        session.commit()

    print(f"Password reset successfully for user '{username}'.")


if __name__ == "__main__":
    main()
