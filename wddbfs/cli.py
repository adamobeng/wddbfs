from cheroot import wsgi
from wsgidav.wsgidav_app import WsgiDAVApp
import logging
import configargparse

import wddbfs.main


def cli():
    p = configargparse.ArgParser(default_config_files=[])
    p.add(
        "-c", "--config", required=False, is_config_file=True, help="config file path"
    )
    p.add("--host", required=False, default="127.0.0.1")
    p.add("--port", required=False, default="8080")
    p.add("--log-level", required=False, default="ERROR")
    p.add(
        "--formats", required=False, default=list(wddbfs.main.TABLE_FORMATTERS.keys())
    )
    p.add("--timeout", required=False, default=0.250)
    p.add(
        "--anonymous", action="store_true", help="allow access without authentication"
    )
    p.add("--username", help="")
    p.add("--password", help="")
    p.add("--db-path", nargs="+", help="paths to sqlite database files")
    p.add(
        "--allow-abspath",
        action="store_true",
        required=False,
        default=False,
        help=(
            "make it possible to access any database on the host filesystem by specifying its absolute path relative to the "
            "WebDAV root (e.g. /mount/webdav/absolute/path/on/host/fs/to/db.sqlite)"
        ),
    )

    options = p.parse_args()

    logger = logging.getLogger("wsgidav")
    logger.propagate = True
    logger.setLevel(getattr(logging, options.log_level))
    logging.basicConfig(
        level=getattr(logging, options.log_level),
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )

    user_mapping = {"*": []}
    if options.username is not None and options.password is not None:
        user_mapping["*"][options.username] = {"password": options.password}
    if options.anonymous:
        user_mapping["*"] = True
    if user_mapping == {"*": []}:
        raise Exception(
            "Either specify a username and password or pass --anonymous to allow unauthenticated access"
        )

    config = {
        "host": options.host,
        "port": int(options.port),
        "provider_mapping": {
            "/": wddbfs.main.DBResourceProvider(
                db_paths=options.db_path or [],
                formats=options.formats,
                allow_abspath=options.allow_abspath,
            ),
        },
        "simple_dc": {"user_mapping": user_mapping},
        "http_authenticator": {},
    }
    app = WsgiDAVApp(config)

    server_args = {
        "bind_addr": (config["host"], config["port"]),
        "wsgi_app": app,
        "timeout": options.timeout,
    }
    server = wsgi.Server(**server_args)
    server.start()
