import unittest
from collections import defaultdict

import cpc_descriptions


class TestCpcDescriptions(unittest.TestCase):
    def test_combine_xml_titles(self):
        code_titles = {
            ("A", 2): "HUMAN NECESSITIES",
            ("A01", 3): "AGRICULTURE",
            ("A21", 3): "FOODSTUFFS; TOBACCO",
            ("B", 2): "PERFORMING OPERATIONS",
            ("B01", 3): "SEPARATING; MIXING",
            ("B21", 3): "SHAPING",
            (
                "A01",
                4,
            ): "AGRICULTURE; FORESTRY; ANIMAL HUSBANDRY; HUNTING; TRAPPING; FISHING",
            ("A21", 4): "BAKING; EDIBLE DOUGH",
            ("B01", 4): "PHYSICAL OR CHEMICAL PROCESSES OR APPARATUS IN GENERAL",
            (
                "B21",
                4,
            ): "MECHANICAL METAL-WORKING WITHOUT ESSENTIALLY REMOVING MATERIAL; PUNCHING METAL",
            ("A01B", 5): "SOIL WORKING IN AGRICULTURE OR FORESTRY; PARTS, DETAILS, OR "
            "ACCESSORIES OF AGRICULTURAL MACHINES OR IMPLEMENTS, IN GENERAL",
            ("A01C", 5): "PLANTING; SOWING; FERTILISING",
            ("A01D", 5): "HARVESTING; MOWING",
            ("B01B", 5): "BOILING; BOILING APPARATUS",
            ("B01D", 5): "SEPARATION",
            ("B01F", 5): "MIXING, e.g. DISSOLVING, EMULSIFYING OR DISPERSING",
        }

        hierarchy = defaultdict(list)
        hierarchy[("A", 2)] = [("A01", 3), ("A21", 3)]
        hierarchy[("A01", 3)] = [("A01", 4)]
        hierarchy[("A21", 3)] = [("A21", 4)]
        hierarchy[("A01", 4)] = [("A01B", 5), ("A01C", 5), ("A01D", 5)]
        hierarchy[("B", 2)] = [("B01", 3), ("B21", 3)]
        hierarchy[("B01", 3)] = [("B01", 4)]
        hierarchy[("B21", 3)] = [("B21", 4)]
        hierarchy[("B01", 4)] = [("B01B", 5), ("B01D", 5), ("B01F", 5)]
        initial_levels = [("A", 2), ("B", 2)]
        final_text = cpc_descriptions.setup_combine_xml_titles(
            hierarchy, code_titles, initial_levels
        )
        all_cpcs = set()
        for row in final_text:
            all_cpcs = all_cpcs.union({row})
            # Make sure the text ends with the specific text of that code
            self.assertTrue(final_text[row]["text"].endswith(code_titles[row]))
            # Make sure it starts with the top-level code
            self.assertTrue(
                final_text[row]["text"].startswith(
                    code_titles[(final_text[row]["code"][0], 2)]
                )
            )
        # Make sure all codes are included
        self.assertEqual(all_cpcs, set(code_titles.keys()))


if __name__ == "__main__":
    unittest.main()
