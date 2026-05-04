"""FastAPI routers for the AMX visualizer.

Each module corresponds to one capability (system, live_db, catalog,
runs, …). The app factory in :mod:`amx.web.server` wires them onto the
``/api`` prefix.
"""
