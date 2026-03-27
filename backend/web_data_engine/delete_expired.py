"""
Script to delete opportunities with expired deadlines from the database
"""

from pipeline.storage.sqlite_db import delete_expired_opportunities


if __name__ == "__main__":
    print("Starting deletion of expired opportunities...")
    delete_expired_opportunities()
    print("Done!")
