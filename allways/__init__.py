__version__ = '3.7.3'
version_split = __version__.split('.')
# Sent as the set_weights version_key; the chain rejects it if below the subnet's weights_version.
__spec_version__ = (1000 * int(version_split[0])) + (10 * int(version_split[1])) + (1 * int(version_split[2]))
