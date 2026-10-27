#!/usr/bin/env python3
"""Image-installed psql interception for production-profile rehearsal."""

from infodengue_pipeline.production.sql_review import main

if __name__ == '__main__':
    raise SystemExit(main())
