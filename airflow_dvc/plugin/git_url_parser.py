# Imports
from collections import defaultdict

from .platforms import PLATFORMS, PLATFORMS_MAP

import sys
sys.path.append("..")
from logs import LOGS

# Possible values to extract from a Git Url
REQUIRED_ATTRIBUTES = (
    "domain",
    "repo",
)


class GitUrlParsed(object):
    def __init__(self, parsed_info):
        self._parsed = parsed_info

        # Set parsed objects as attributes
        for k, v in parsed_info.items():
            setattr(self, k, v)

    def _valid_attrs(self):
        return all([getattr(self, attr, None) for attr in REQUIRED_ATTRIBUTES])

    @property
    def valid(self):
        return all(
            [
                self._valid_attrs(),
            ]
        )

    @property
    def _platform_obj(self):
        return PLATFORMS_MAP[self.platform]

    ##
    # Alias properties
    ##
    @property
    def host(self):
        return self.domain

    @property
    def user(self):
        if hasattr(self, "_user"):
            return self._user

        return self.owner

    ##
    # Format URL to protocol
    ##
    def format(self, protocol):
        return self._platform_obj.FORMATS[protocol] % self._parsed

    ##
    # Normalize
    ##
    @property
    def normalized(self):
        return self.format(self.protocol)

    ##
    # Rewriting
    ##
    @property
    def url2ssh(self):
        return self.format("ssh")

    @property
    def url2http(self):
        return self.format("http")

    @property
    def url2https(self):
        return self.format("https")

    @property
    def url2git(self):
        return self.format("git")

    # All supported Urls for a repo
    @property
    def urls(self):
        return dict(
            (protocol, self.format(protocol))
            for protocol in self._platform_obj.PROTOCOLS
        )

    ##
    # Platforms
    ##
    @property
    def github(self):
        return self.platform == "github"

    @property
    def bitbucket(self):
        return self.platform == "bitbucket"

    @property
    def friendcode(self):
        return self.platform == "friendcode"

    @property
    def assembla(self):
        return self.platform == "assembla"

    @property
    def gitlab(self):
        return self.platform == "gitlab"

    ##
    # Get data as dict
    ##
    @property
    def data(self):
        return dict(self._parsed)


SUPPORTED_ATTRIBUTES = (
    "domain",
    "repo",
    "owner",
    "_user",
    "port",
    "url",
    "platform",
    "protocol",
)


def _parse(url: str, check_domain: bool = True):
    # Values are None by default
    parsed_info = defaultdict(lambda: None)
    parsed_info["port"] = ""
    LOGS.git_url_parser.info(f"Parse GIT url: {url}")

    # Defaults to all attributes
    map(parsed_info.setdefault, SUPPORTED_ATTRIBUTES)

    for name, platform in PLATFORMS:
        for protocol, regex in platform.COMPILED_PATTERNS.items():
            # Match current regex against URL
            match = regex.match(url)

            # Skip if not matched
            if not match:
                LOGS.git_url_parser.info(f"GIT url {url} not matched by {regex.pattern} for platform {platform}")
                continue

            # Skip if domain is bad
            domain = match.group("domain")
            if "@" in domain:
                domain = domain.split("@")[-1]
            LOGS.git_url_parser.info(f"GIT url domain is: {domain}")
            if check_domain:
                if platform.DOMAINS and not (domain in platform.DOMAINS):
                    LOGS.git_url_parser.info(f"GIT url domain {domain} not listed in {' ,'.join(platform.DOMAINS)}")
                    continue

            # Get matches as dictionary
            matches = match.groupdict()

            # Update info with matches
            parsed_info.update(matches)

            # add in platform defaults
            parsed_info.update(platform.DEFAULTS)

            # Update info with platform info
            parsed_info.update(
                {
                    "url": url,
                    "platform": name,
                    "protocol": protocol,
                }
            )
            LOGS.git_url_parser.info(f"Correctly parsed GIT url {url}")
            return parsed_info

    # Empty if none matched
    LOGS.git_url_parser.info(f"Invalid URL {url}")
    return parsed_info


def parse(url, check_domain=True):
    return GitUrlParsed(_parse(url, check_domain))


def validate(url, check_domain=True):
    return parse(url, check_domain).valid
