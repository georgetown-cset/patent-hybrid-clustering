import unittest

import regex

import archive.scripts.translate_patents as translate_patents


class TestTranslate(unittest.TestCase):
    def test_init(self):
        translate = translate_patents.Translator("../data/test_translated.jsonl")
        self.assertEqual(translate.max_chars_to_translate, 25 * 10**6)
        self.assertEqual(translate.patents, [])
        self.assertEqual(translate.re_bad_chars, regex.compile(r"[\p{Cc}\p{Cs}]+"))
        self.assertEqual(translate.output_file, "../data/test_translated.jsonl")
        self.assertEqual(translate.num_chars_translated, 0)

    def test_translate_text(self):
        translate = translate_patents.Translator("../data/test_translated.jsonl")
        text = (
            "Fremgangsmåte for konservering av fullmodne poteter og "
            "andre rotfrukter ved hjelp av dypfrysning."
        )
        result = translate.translate_text(text)
        self.assertNotEqual(text, result)
        self.assertEqual(
            result,
            "Method for preserving fully ripe potatoes "
            "and other root vegetables by deep freezing.",
        )
        # test character max
        translate.max_chars_to_translate = 5
        self.assertRaises(ValueError, translate.translate_text, text)

    def test_remove_bad_chars(self):
        translate = translate_patents.Translator("../data/test_translated.jsonl")
        bad_text = (
            "Fremgangsmåte \x96for konservering av fullmodne poteter og "
            "andre rotfrukter ved hjelp av dypfrysning."
        )
        fixed_text = (
            "Fremgangsmåte for konservering av fullmodne poteter og "
            "andre rotfrukter ved hjelp av dypfrysning."
        )
        new_text = translate.remove_bad_chars(bad_text)
        self.assertNotEqual(bad_text, new_text)
        self.assertEqual(fixed_text, new_text)

    def test_validate_translation(self):
        translate = translate_patents.Translator("../data/test_translated.jsonl")
        text = (
            "Method for the preservation of fully ripe potatoes "
            "and other root vegetables by means of deep freezing."
        )
        self.assertTrue(translate.validate_translation(text))
        non_english_text = (
            "Fremgangsmåte for konservering av fullmodne poteter og "
            "andre rotfrukter ved hjelp av dypfrysning."
        )
        self.assertFalse(translate.validate_translation(non_english_text))


if __name__ == "__main__":
    unittest.main()
