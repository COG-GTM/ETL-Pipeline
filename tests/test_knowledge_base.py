import json
import os
import tempfile

import pytest

from src.domain_learner.knowledge_base import DomainKnowledgeBase


class TestDomainKnowledgeBase:
    """Tests for the DomainKnowledgeBase class."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.kb = DomainKnowledgeBase(storage_dir=self.tmpdir)

    def _make_profile(self, source, columns, row_count=100):
        return {
            "source": source,
            "row_count": row_count,
            "columns": columns,
            "summary": {},
        }

    def test_init_creates_storage_dir(self):
        """Should create storage directory on init."""
        assert os.path.isdir(self.tmpdir)

    def test_init_default_knowledge_structure(self):
        """Should have correct default knowledge structure."""
        assert self.kb.knowledge["domain"] is None
        assert self.kb.knowledge["entities"] == {}
        assert self.kb.knowledge["relationships"] == []
        assert self.kb.knowledge["business_rules"] == []
        assert self.kb.knowledge["data_patterns"] == []
        assert self.kb.knowledge["glossary"] == {}

    def test_learn_from_profiles_basic(self):
        """Should learn entities from profiles."""
        profiles = [
            self._make_profile("customers.csv", {
                "customer_id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
                "name": {"dtype": "object", "null_count": 0, "semantic_type": None, "inferred_role": "text"},
            }),
        ]
        result = self.kb.learn_from_profiles(profiles)

        assert "customers" in result["entities"]
        assert result["entities"]["customers"]["row_count"] == 100
        assert "customer_id" in result["entities"]["customers"]["attributes"]

    def test_learn_from_profiles_detects_relationships(self):
        """Should detect relationships based on shared _id columns."""
        profiles = [
            self._make_profile("orders.csv", {
                "order_id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
                "customer_id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": None},
            }),
            self._make_profile("customers.csv", {
                "customer_id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
                "name": {"dtype": "object", "null_count": 0, "semantic_type": None, "inferred_role": "text"},
            }),
        ]
        result = self.kb.learn_from_profiles(profiles)

        assert len(result["relationships"]) > 0
        rel = result["relationships"][0]
        assert rel["join_column"] == "customer_id"
        assert rel["relationship_type"] == "foreign_key"

    def test_infer_domain_ecommerce(self):
        """Should infer e-commerce domain from relevant column names."""
        entities = {
            "orders": {
                "attributes": {
                    "order_id": {}, "product_id": {}, "customer_id": {},
                    "price": {}, "payment_method": {},
                },
            },
        }
        domain = self.kb._infer_domain(entities)
        assert domain in ("e-commerce", "retail")

    def test_infer_domain_healthcare(self):
        """Should infer healthcare domain."""
        entities = {
            "records": {
                "attributes": {
                    "patient_id": {}, "diagnosis_code": {},
                    "treatment_plan": {}, "prescription_id": {},
                },
            },
        }
        domain = self.kb._infer_domain(entities)
        assert domain == "healthcare"

    def test_infer_domain_general(self):
        """Should return 'general' when no domain is detected."""
        entities = {
            "data": {
                "attributes": {"col_a": {}, "col_b": {}, "col_c": {}},
            },
        }
        domain = self.kb._infer_domain(entities)
        assert domain == "general"

    def test_build_glossary(self):
        """Should build glossary from known terms."""
        entities = {
            "users": {
                "attributes": {
                    "customer_id": {}, "email": {}, "phone": {},
                },
            },
        }
        glossary = self.kb._build_glossary(entities)

        assert "customer_id" in glossary
        assert "email" in glossary

    def test_infer_business_rules_positive_prices(self):
        """Should infer rules for price columns."""
        profiles = [
            self._make_profile("sales.csv", {
                "sale_price": {
                    "dtype": "float64", "null_count": 0,
                    "semantic_type": None, "inferred_role": None,
                    "stats": {"negatives": 0},
                },
            }),
        ]
        rules = self.kb._infer_business_rules({}, profiles)

        price_rules = [r for r in rules if "sale_price" in r.get("column", "")]
        assert len(price_rules) > 0
        assert any("positive" in r["rule"].lower() for r in price_rules)

    def test_infer_business_rules_negative_prices(self):
        """Should flag negative prices for validation."""
        profiles = [
            self._make_profile("sales.csv", {
                "total_amount": {
                    "dtype": "float64", "null_count": 0,
                    "semantic_type": None, "inferred_role": None,
                    "stats": {"negatives": 5},
                },
            }),
        ]
        rules = self.kb._infer_business_rules({}, profiles)

        amount_rules = [r for r in rules if "total_amount" in r.get("column", "")]
        assert any("non-negative" in r["rule"].lower() or "refund" in r["rule"].lower() for r in amount_rules)

    def test_infer_business_rules_required_id(self):
        """Should infer NOT NULL constraint for _id columns with 0 nulls."""
        profiles = [
            self._make_profile("orders.csv", {
                "order_id": {
                    "dtype": "int64", "null_count": 0,
                    "semantic_type": None, "inferred_role": None,
                },
            }),
        ]
        rules = self.kb._infer_business_rules({}, profiles)

        id_rules = [r for r in rules if "order_id" in r.get("column", "")]
        assert any("required" in r["rule"].lower() or "not null" in r["rule"].lower() for r in id_rules)

    def test_infer_business_rules_email_format(self):
        """Should infer email format rule."""
        profiles = [
            self._make_profile("users.csv", {
                "user_email": {
                    "dtype": "object", "null_count": 0,
                    "semantic_type": "email", "inferred_role": None,
                },
            }),
        ]
        rules = self.kb._infer_business_rules({}, profiles)

        email_rules = [r for r in rules if r.get("type") == "format"]
        assert len(email_rules) > 0

    def test_detect_patterns_duplicates(self):
        """Should detect duplicate data pattern."""
        profiles = [
            {
                "source": "data.csv",
                "summary": {"duplicate_rows": 10, "null_percentage": 0},
            },
        ]
        patterns = self.kb._detect_patterns(profiles)

        assert len(patterns) == 1
        assert patterns[0]["pattern"] == "duplicate_data"

    def test_detect_patterns_sparse_data(self):
        """Should detect sparse data pattern (high null percentage)."""
        profiles = [
            {
                "source": "data.csv",
                "summary": {"duplicate_rows": 0, "null_percentage": 25},
            },
        ]
        patterns = self.kb._detect_patterns(profiles)

        assert len(patterns) == 1
        assert patterns[0]["pattern"] == "sparse_data"

    def test_detect_patterns_no_issues(self):
        """Should return empty when no patterns detected."""
        profiles = [
            {
                "source": "data.csv",
                "summary": {"duplicate_rows": 0, "null_percentage": 2},
            },
        ]
        patterns = self.kb._detect_patterns(profiles)
        assert len(patterns) == 0

    def test_save_knowledge(self):
        """Should save knowledge to JSON file."""
        profiles = [
            self._make_profile("test.csv", {
                "id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
            }),
        ]
        self.kb.learn_from_profiles(profiles)
        filepath = self.kb.save_knowledge()

        assert os.path.exists(filepath)
        with open(filepath, "r") as f:
            saved = json.load(f)
        assert "entities" in saved
        assert saved["domain"] is not None

    def test_generate_report(self):
        """Should generate a formatted report string."""
        profiles = [
            self._make_profile("customers.csv", {
                "customer_id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
                "email": {"dtype": "object", "null_count": 0, "semantic_type": "email", "inferred_role": "text"},
            }),
        ]
        self.kb.learn_from_profiles(profiles)
        report = self.kb.generate_report()

        assert "DOMAIN KNOWLEDGE REPORT" in report
        assert "customers" in report

    def test_learned_at_timestamp(self):
        """Should record timestamp when learning is complete."""
        profiles = [
            self._make_profile("test.csv", {
                "id": {"dtype": "int64", "null_count": 0, "semantic_type": None, "inferred_role": "identifier"},
            }),
        ]
        result = self.kb.learn_from_profiles(profiles)
        assert result["learned_at"] is not None
