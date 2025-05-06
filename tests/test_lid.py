import unittest

import archive.scripts.lid_patents as lid_patents
import scripts.lid_new_patents as lid_new_patents

class TestLid(unittest.TestCase):

    def test_remove_bad_chars(self):
        for lib in [lid_patents, lid_new_patents]:
            char_string = "hello\r\n"
            new_string = lib.remove_bad_chars(char_string)
            self.assertEqual(new_string, "hello")

    def test_lid(self):
        data = [
            {   "patent_id": "NO-144956-B",
                "family_id": "3570204",
                "abstract_original": "Fremgangsmåte for termisk cyanid-avgiftning. " \
                                     "av katodekullet fra aluminium-elektrolyseceller."
            },
            {"patent_id": "IT-MI941467-A1",
             "family_id": "8537043",
             "abstract_original": "L'invenzione riguarda uno sci prodotto con una pluralità di parti "
                                  "funzionali separate tra loro, che sono costituite da una parte "
             },
             {"patent_id": "GB-2317415-A",
             "family_id": "8546691",
             "abstract_original": "A slatted roll-up door is braked by a roller 2, or a non-rotating member, "
                                  "that is spring-biased against the inner face of the door."
             },
             {"patent_id": "TW-D214711-S",
             "family_id": "77913749",
             "abstract_original": "【物品用途】;本設計物品是一種電子連接器之端子位置定位裝置(Terminal Position "
                                  "Assurance, TPA)。;【設計說明】;無。"
             }

        ]
        results = lid_new_patents.lid(data)
        self.assertEqual(results[0]["patent_id"], "NO-144956-B")
        self.assertEqual(results[0]["family_id"], "3570204")
        self.assertTrue(results[0]["reliable"])
        self.assertEqual(results[0]["language"], "no")
        self.assertEqual(results[0]["language_long"], "NORWEGIAN")
        self.assertEqual(results[1]["patent_id"], "IT-MI941467-A1")
        self.assertEqual(results[1]["family_id"], "8537043")
        self.assertTrue(results[1]["reliable"])
        self.assertEqual(results[1]["language"], "it")
        self.assertEqual(results[1]["language_long"], "ITALIAN")
        self.assertEqual(results[2]["patent_id"], "GB-2317415-A")
        self.assertEqual(results[2]["family_id"], "8546691")
        self.assertTrue(results[2]["reliable"])
        self.assertEqual(results[2]["language"], "en")
        self.assertEqual(results[2]["language_long"], "ENGLISH")
        self.assertEqual(results[3]["patent_id"], "TW-D214711-S")
        self.assertEqual(results[3]["family_id"], "77913749")
        self.assertFalse(results[3]["reliable"])
        self.assertEqual(results[3]["language"], "zh-Hant")
        self.assertEqual(results[3]["language_long"], "ChineseT")





if __name__ == "__main__":
    unittest.main()


