# mudclient

A browser MUD client with an auto-mapper. A stdlib-only Python server bridges WebSockets to telnet and serves the page.

    python server.py            # opens http://127.0.0.1:8765

Enter a host and port and connect. Movement commands (n, s, e, w, ne, nw, se, sw, u, d) are mapped automatically; click a room to rename it or walk to it. Maps are saved per server in the browser and can be exported or imported.
