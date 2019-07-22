"""
Copyright (C) 2018 Ocean Observatories Initiative.
"""

import re
import os
import json
import subprocess


class Note:
    def __init__(self, title, ticket):
        self.title = title
        self.ticket = ticket
        self.details = None


class Version:
    def __init__(self, descriptor, number, date):
        self.descriptor = descriptor
        self.number = number
        self.date = date
        self.details = None
        self.notes = []


class Component:
    def __init__(self):
        self.name = None
        self.details = None
        self.dependencies = []
        self.versions = []


class ComponentDecoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Component):
            return dict(name=obj.name, details=obj.details, dependencies=obj.dependencies, versions=obj.versions)
        elif isinstance(obj, Version):
            return dict(descriptor=obj.descriptor, number=obj.number, release=obj.date, details=obj.details,
                        notes=obj.notes)
        elif isinstance(obj, Note):
            return dict(title=obj.title, ticket=obj.ticket, details=obj.details)
        else:
            return json.JSONEncoder.default(self, obj)


class ReleaseNotes:
    """
    Python Object representation of markdown formatted release notes.
    """

    # default value for the Release Notes file
    DEFAULT_FILE = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "../",
        "RELEASE_NOTES.md")

    # value for when the version cannot be determined
    UNKNOWN_VERSION = "0.0.1-unknown"

    # Matches the component name that should be the first header of the release notes
    # for example: "# Stream Engine"
    _COMPONENT_NAME_PATTERN = re.compile(r'#\s+(?P<name>.*)')

    # Matches Patterns such as: "# Development Release 1.0.0 2018-11-01"
    _VERSION_PATTERN = re.compile(r'#\s+(?P<descriptor>[\w\s]+)\s+(?P<version>\S+)\s+(?P<date>\S*)\s*')

    _NOTE_PATTERN = re.compile(r'[\s+]?[Issue|Story]\s+#(?P<ticket>[\d]+)\s*')

    # singleton instance to use so that file isn't double parsed
    __instance = None

    @staticmethod
    def instance(file=DEFAULT_FILE):
        if not ReleaseNotes.__instance:
            ReleaseNotes.__instance = ReleaseNotes(file)
        return ReleaseNotes.__instance

    def __init__(self, file=DEFAULT_FILE):
        self.file = file
        self._contents = None
        self._dependencies = None
        self._component = Component()
        self._parsed = False

    def contents(self):
        """
        :return: the contents of the file as a list of strings, or None if the contents couldn't
        be read
        """
        if not self._contents:
            with open(self.file, 'r') as file_handle:
                self._contents = file_handle.readlines()
        return self._contents

    def dependencies(self):
        """
        :return: the environment package requirements as a list of strings
        """
        if not self._dependencies:
            # -e makes conda list show the output requirement string only (i.e. no build or channel info)
            byte_output = subprocess.check_output(['conda', 'list', '-e'])
            output = byte_output.decode("utf-8").split("\n")
            # remove some nuissance header lines by excluding all comment lines from output
            self._dependencies = [line for line in output if len(line) > 0 and line[0] != "#"]
        return self._dependencies

    def _parse(self):
        """
        :return: True if the file has been parsed, False if something went wrong.
        """
        if not self._parsed and self.contents():
            # tracks what component/version/note a detail line describes
            last_object = self._component
            # track details line for an object (component/version/note)
            details_buffer = []

            for line in self.contents():

                # find component name
                if not self._component.name:
                    match = re.search(ReleaseNotes._COMPONENT_NAME_PATTERN, line)
                    if match:
                        self._component.name = match.group("name")
                    continue

                # parse version lines
                version_match = re.search(ReleaseNotes._VERSION_PATTERN, line)
                if version_match:
                    descriptor = version_match.group("descriptor")
                    number = version_match.group("version")
                    date = version_match.group("date")
                    version = Version(descriptor, number, date)
                    self._component.versions.append(version)
                    self._flush_details(details_buffer, last_object)
                    details_buffer = []
                    last_object = version
                    continue

                # parse note version lines
                note_match = re.search(ReleaseNotes._NOTE_PATTERN, line)
                if note_match:
                    title = line.rstrip().lstrip()
                    ticket = note_match.group("ticket")
                    note = Note(title, ticket)
                    self._component.versions[-1].notes.append(note)
                    self._flush_details(details_buffer, last_object)
                    details_buffer = []
                    last_object = note
                    continue

                # not the component name, nor a version, nor a note - treat as a detail line
                details_buffer.append(line)
            self._flush_details(details_buffer, last_object)
            self._parsed = True

        # if dependencies not already set and there are dependencies to add...
        if not self._dependencies and self.dependencies():
            self._component.dependencies = self._dependencies

        return self._parsed

    @staticmethod
    def _flush_details(details_buffer, object_with_details):
        # strip extraneous whitespace at beginning and end of combined string
        details = "".join(details_buffer).rstrip().lstrip() if details_buffer else None
        if details:
            object_with_details.details = details

    def get_component_name(self):
        """
        :return: the component name or None if no component name was found
        """
        self._parse()
        return self._component.name

    def get_latest_version(self):
        """
        :return: the latest version or UNKNOWN_VERSION if no version is found
        """
        self._parse()
        return self._component.versions[0].number if self._component.versions else ReleaseNotes.UNKNOWN_VERSION

    def get_latest_date(self):
        """
        :return: the latest date or None is no date is found
        """
        self._parse()
        return self._component.versions[0].date if self._component.versions else None

    def get_latest_descriptor(self):
        """
        :return: the latest descriptor or None if no descriptor is found
        """
        self._parse()
        return self._component.versions[0].descriptor if self._component.versions else None

    def get_component(self):
        """
        :return: the component representing Stream Engine version information
        """
        self._parse()
        return self._component

    def get_dependencies(self):
        """
        :return: the list of Strings representing required packages and their versions
        """
        self._parse()
        return self._dependencies