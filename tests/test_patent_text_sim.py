import unittest
import scripts.patent_text_sim as patent_text_sim
from collections import defaultdict

class TestTextSim(unittest.TestCase):

    def test_init(self):
        post = patent_text_sim.Postprocessor()
        self.assertEqual(type(post.already_seen), type(defaultdict(set)))
        self.assertEqual(len(post.already_seen), 0)

    def test_build_already_seen(self):
        post = patent_text_sim.Postprocessor()
        post.build_already_seen(0, "lithium ion battery")
        # Confirm we're adding correctly
        self.assertIn(0, post.already_seen)
        self.assertIn("lithium", post.already_seen[0])
        self.assertIn("ion", post.already_seen[0])
        self.assertIn("battery", post.already_seen[0])
        self.assertEqual(len(post.already_seen[0]), 3)
        # Confirm we only add new words, not previous ones
        post.build_already_seen(0, "rechargeable lithium battery")
        self.assertIn("rechargeable", post.already_seen[0])
        self.assertIn("lithium", post.already_seen[0])
        self.assertIn("battery", post.already_seen[0])
        self.assertEqual(len(post.already_seen[0]), 4)
        # Confirm we add to different clusters
        post.build_already_seen(1, "continuous zoom lens")
        self.assertIn("continuous", post.already_seen[1])
        self.assertIn("zoom", post.already_seen[1])
        self.assertIn("lens", post.already_seen[1])
        self.assertEqual(len(post.already_seen[1]), 3)
        self.assertEqual(len(post.already_seen), 2)

    def test_remove_generics(self):
        post = patent_text_sim.Postprocessor()
        # Test don't do anything
        cleaned = post.remove_generics(0, "lithium ion battery")
        self.assertEqual(cleaned, "lithium ion battery")
        # Test remove repeat words
        cleaned = post.remove_generics(0, "lithium ion ion")
        self.assertEqual(cleaned, "lithium ion")
        cleaned = post.remove_generics(0, "lithium lithium ion")
        self.assertEqual(cleaned, "lithium ion")
        # Test remove previously seen
        post.build_already_seen(0, "lithium ion battery")
        cleaned = post.remove_generics(0, "lithium")
        self.assertIsNone(cleaned)
        cleaned = post.remove_generics(0, "ion")
        self.assertIsNone(cleaned)
        cleaned = post.remove_generics(0, "battery")
        self.assertIsNone(cleaned)
        # Test remove stopwords
        cleaned = post.remove_generics(0, "this")
        self.assertIsNone(cleaned)
        cleaned = post.remove_generics(0, "where")
        self.assertIsNone(cleaned)
        cleaned = post.remove_generics(0, "yes")
        self.assertIsNone(cleaned)

if __name__ == '__main__':
    unittest.main()
