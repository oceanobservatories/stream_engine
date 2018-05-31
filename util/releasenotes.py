"""
Copyright (C) 2018 Ocean Observatories Initiative.
"""

import re

class ReleaseNotes:
    """
    Python Object representation of markdown formatted release notes.
    """

    # default value for the Release Notes file
    DEFAULT_FILE = "RELEASE_NOTES.md"

    # value for when the version cannot be determined
    UNKNOWN_VERSION = "0.0.1-unknown"

    # Matches the component name that should be the first header of the release notes
    # for example: "# Stream Engine"
    _COMPONENT_NAME_PATTERN = re.compile("#\s+(?P<name>.*)");

    # Matches Patterns such as: "# Development Release 1.0.0 2018-11-01"
    _VERSION_PATTERN = re.compile("#\s+(?P<descriptor>[\w\s]+)\s+(?P<version>\S+)\s+(?P<date>\S*)\s*")

    def __init__(self,file=DEFAULT_FILE):
        self.file = file
        self._contents = None
        self._component_name = None
        self._latest_version = ReleaseNotes.UNKNOWN_VERSION
        self._parsed = False

    def contents(self):
        """
        :return: the contents of the file as a list of strings, or None if the contents couldn't
        be read
        """
        if not self._contents:
            with open(self.file, "r") as file_handle:
                self._contents = file_handle.readlines()
        return self._contents

    def _parse(self):
        """
        :return: True if the file has been parsed, False if something went wrong.
        """
        if not self._parsed and self.contents():
            for line in self.contents():

                # find component name
                match = re.search(ReleaseNotes._COMPONENT_NAME_PATTERN,line)
                if match and not self._component_name:
                    self._component_name = match.group("name")
                    continue

                # find latest version
                match = re.search(ReleaseNotes._VERSION_PATTERN,line)
                if match and self._component_name and self._latest_version == ReleaseNotes.UNKNOWN_VERSION:
                    self._latest_version = match.group("version")
                    continue

                # parsed all we want to know right now, perform quick return
                if self._component_name and self._latest_version != ReleaseNotes.UNKNOWN_VERSION:
                    break

            self._parsed = True
        return self._parsed

    def latest_version(self):
        """
        :return: the latest version or UNKNOWN_VERSION if no version is found
        """
        self._parse()
        return self._latest_version

