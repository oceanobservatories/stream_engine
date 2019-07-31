
import os
import mock
import json
import logging
import unittest

from util.releasenotes import ReleaseNotes, ComponentDecoder


TEST_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(TEST_DIR, 'data')
FAKE_RELEASE_NOTES = os.path.join(DATA_DIR, 'TEST_RELEASE_NOTES.md')


logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class ReleaseNotesTest(unittest.TestCase):
    def setUp(self):
        self.release = ReleaseNotes(FAKE_RELEASE_NOTES)

    def test_parse_current_release_notes(self):
        actual_release = ReleaseNotes(self.release.DEFAULT_FILE)
        component = actual_release.component
        self.assertIsNotNone(component)

    def test_get_component_name(self):
        name = self.release.component_name
        self.assertEqual(name, "Stream Engine")

    def test_get_latest_version(self):
        version_number = self.release.latest_version
        self.assertEqual(version_number, "1.10.0")

    def test_get_latest_date(self):
        date = self.release.latest_date
        self.assertEqual(date, "2019-07-19")

    def test_get_latest_descriptor(self):
        descriptor = self.release.latest_descriptor
        self.assertEqual(descriptor, "Development Release")

    def test_get_component(self):
        component = self.release.component
        self.assertIsNotNone(component)

    def test_get_dependencies(self):
        dependencies = self.release.dependencies
        package_names = [dependency.split("=")[0] for dependency in dependencies]
        self.assertIsNotNone(dependencies)
        self.assertIn("mock", package_names)

    def test_component_serialization(self):
        component = self.release.component
        serialized_component = json.dumps(component, cls=ComponentDecoder, indent=2, separators=(',', ': '))
        self.assertIsNotNone(serialized_component)

    def test_known_version(self):
        component = self.release.component
        version = component.versions[4]
        self.assertEqual(version.descriptor, "Release")
        self.assertEqual(version.number, "1.6.0")
        self.assertEqual(version.date, "2018-07-20")
        self.assertIsNone(version.details)
        self.assertIs(len(version.notes), 5)

    def test_known_note(self):
        component = self.release.component
        version = component.versions[4]
        note = version.notes[0]
        self.assertEqual(note.title, "Issue #13448 - Prevent getting fill values for wavelength coordinate")
        self.assertEqual(note.ticket, "13448")
        self.assertEqual(note.details, "- Correct code error that led to NaNs for wavelength values\n- Add handling "
                                       "to replace coordinate fill values with whole number sequence")

    def test_version_without_date(self):
        component = self.release.component
        version = component.versions[18]
        self.assertEqual(version.number, "1.3.0")
        self.assertEqual(version.date, "")

    def test_version_without_notes(self):
        component = self.release.component
        version = component.versions[18]
        self.assertEqual(version.number, "1.3.0")
        self.assertFalse(version.notes)
        self.assertEqual(version.details, "Updated required packages\n- numpy\n- numexpr\n- scipy\n- ooi-data\n- "
                                          "flask\n- gunicorn\n- xarray\n- pandas")

    def test_note_without_details(self):
        component = self.release.component
        version = component.versions[20]
        note = version.notes[0]
        self.assertEqual(version.number, "1.2.9")
        self.assertIsNone(note.details)
        self.assertEqual(note.title, "Issue #12040 - parameter resolution error")

    def test_note_with_story_label(self):
        component = self.release.component
        version = component.versions[-1]
        note = version.notes[-1]
        self.assertEqual(version.number, "1.0.2")
        self.assertIs(len(version.notes), 3)
        self.assertEqual(note.ticket, "1007")

    @mock.patch("util.releasenotes.ReleaseNotes._parse")
    def test_unknown_version(self, mock_parse):
        mock_parse.return_value = None
        version = self.release.latest_version
        self.assertEqual(version, "0.0.1-unknown")
