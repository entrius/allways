"""``python -m allways.swap_api`` — uvicorn entrypoint for local dev and prod."""

import os

import uvicorn


def main() -> None:
    port = int(os.environ.get('SWAP_API_PORT', '8000'))
    host = os.environ.get('SWAP_API_HOST', '0.0.0.0')
    uvicorn.run('allways.swap_api.app:app', host=host, port=port, log_level='info')


if __name__ == '__main__':
    main()
