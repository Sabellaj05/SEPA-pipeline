#!/usr/bin/env python3
"""
Main entry point for the SEPA pipeline.
"""
import asyncio

from src.sepa_pipeline.main import main

if __name__ == "__main__":
     asyncio.run(main())
