"""Tests for GPU type names.

Run: uv run pytest tests/test_gpu_names.py

Covers how one name written several ways finds a provider's own type, which of several acceptable types a provider
uses, and what a resource request may say about GPU types.
"""

import pytest

from benchmark_service.sandbox.gpu import daytona_gpu_type, modal_gpu_type, normalize_gpu_name
from benchmark_service.sandbox.types import Resources, SandboxError


class TestNormalizeGpuName:
    """Case, vendor prefixes and separators don't tell one GPU from another."""

    @pytest.mark.parametrize(
        ("name", "key"),
        [
            ("H100", "h100"),
            ("NVIDIA H100 80GB", "h100-80gb"),
            ("nvidia_h100_80gb", "h100-80gb"),
            ("NVIDIA-GeForce-RTX-4090", "rtx-4090"),
            ("  RTX_5090 ", "rtx-5090"),
        ],
    )
    def test_the_key_is_lowercase_without_a_vendor_prefix_and_with_dashes(self, name: str, key: str) -> None:
        assert normalize_gpu_name(name) == key


class TestDaytonaGpuType:
    """Daytona offers a fixed set of types, so a name must map to one of them."""

    @pytest.mark.parametrize(
        ("name", "native"),
        [
            ("H100", "H100"),
            ("nvidia-h100-80gb", "H100"),
            ("h200", "H200"),
            ("RTX-4090", "RTX-4090"),
            ("nvidia-geforce-rtx-4090", "RTX-4090"),
            ("rtx5090", "RTX-5090"),
            ("RTX PRO 6000", "RTX-PRO-6000"),
        ],
    )
    def test_a_name_in_any_common_form_maps_to_daytonas_type(self, name: str, native: str) -> None:
        assert daytona_gpu_type([name]) == native

    def test_the_first_acceptable_type_daytona_offers_is_used_and_the_others_are_skipped(self) -> None:
        assert daytona_gpu_type(["A100", "T4", "nvidia-h100-80gb", "RTX-4090"]) == "H100"

    def test_no_types_means_any(self) -> None:
        assert daytona_gpu_type([]) is None

    def test_types_daytona_does_not_offer_are_refused_with_what_it_does(self) -> None:
        with pytest.raises(
            SandboxError, match=r"Unsupported Daytona GPU types: A100, T4\. Supported types: H100, H200"
        ):
            daytona_gpu_type(["A100", "T4"])

    def test_a_single_unsupported_type_is_named_in_the_singular(self) -> None:
        with pytest.raises(SandboxError, match=r"Unsupported Daytona GPU type: T4\."):
            daytona_gpu_type(["T4"])


class TestModalGpuType:
    """Modal accepts new types as they appear, so only the names it is known by are rewritten."""

    @pytest.mark.parametrize(
        ("name", "native"),
        [
            ("h100", "H100"),
            ("NVIDIA H100 80GB", "H100"),
            ("a100", "A100-40GB"),
            ("A100-80GB", "A100-80GB"),
            ("nvidia-l40s", "L40S"),
        ],
    )
    def test_a_known_name_maps_to_modals_type(self, name: str, native: str) -> None:
        assert modal_gpu_type([name]) == native

    def test_a_name_modal_may_know_better_is_passed_through_as_written(self) -> None:
        assert modal_gpu_type(["B300"]) == "B300"

    def test_the_first_type_is_the_one_asked_for(self) -> None:
        assert modal_gpu_type(["nvidia-h100-80gb", "a100"]) == "H100"

    def test_no_types_gives_none(self) -> None:
        assert modal_gpu_type([]) is None


class TestResourcesGpuTypes:
    """A request names its acceptable GPU types one way or the other, and only with GPUs."""

    def test_the_acceptable_types_are_the_list_or_the_single_type(self) -> None:
        assert Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_types=["H100", "A100"]).acceptable_gpu_types == [
            "H100",
            "A100",
        ]
        assert Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_type="H100").acceptable_gpu_types == ["H100"]
        assert Resources(vcpu=1, memory=1, disk=1).acceptable_gpu_types == []

    def test_a_list_and_a_single_type_together_are_refused(self) -> None:
        with pytest.raises(ValueError, match="gpu_type or gpu_types, not both"):
            Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_type="H100", gpu_types=["A100"])

    def test_types_without_a_gpu_count_are_refused(self) -> None:
        with pytest.raises(ValueError, match="gpu_type requires gpu >= 1"):
            Resources(vcpu=1, memory=1, disk=1, gpu_types=["H100"])
