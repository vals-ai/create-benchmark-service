"""Tests for GPU type names.

Run: uv run pytest tests/test_gpu_names.py

Covers how one name written several ways finds a provider's own type, which of several acceptable types each provider
asks for, and what a resource request may say about GPU types.
"""

import pytest
from daytona import GpuType

from benchmark_service.sandbox.gpu import daytona_gpu_type, modal_gpu_type, normalize_gpu_name
from benchmark_service.sandbox.types import Resources, SandboxError


class TestNormalizeGpuName:
    """Case, vendor prefixes and separators don't tell one GPU from another."""

    @pytest.mark.parametrize(
        ("name", "key"),
        [
            ("H100", "h100"),
            ("NVIDIA H100 80GB", "h100"),
            ("nvidia_h100_80gb", "h100"),
            ("NVIDIA-GeForce-RTX-4090", "rtx-4090"),
            ("nvidia-tesla-t4", "t4"),
            ("  RTX_5090 ", "rtx-5090"),
            ("rtx-_4090", "rtx-4090"),
            ("RTX PRO 6000", "rtx-pro-6000"),
        ],
    )
    def test_the_key_is_lowercase_without_a_vendor_prefix_and_with_one_dash_between_words(
        self, name: str, key: str
    ) -> None:
        assert normalize_gpu_name(name) == key


class TestDaytonaGpuType:
    """Daytona offers a fixed set of types, so a name must map to one of them."""

    @pytest.mark.parametrize(
        ("name", "native"),
        [
            ("H100", GpuType.H100),
            ("nvidia-h100-80gb", GpuType.H100),
            ("h200", GpuType.H200),
            ("nvidia-geforce-rtx-4090", GpuType.RTX_4090),
            ("rtx5090", GpuType.RTX_5090),
            ("RTX PRO 6000", GpuType.RTX_PRO_6000),
        ],
    )
    def test_a_name_in_any_common_form_maps_to_daytonas_type(self, name: str, native: GpuType) -> None:
        assert daytona_gpu_type([name]) == native

    def test_every_type_daytona_offers_is_accepted_by_its_own_name(self) -> None:
        offered = [member for member in GpuType if member is not GpuType.UNKNOWN_DEFAULT_OPEN_API]

        assert [daytona_gpu_type([member.value]) for member in offered] == offered

    def test_types_daytona_does_not_offer_are_refused_with_what_it_does(self) -> None:
        with pytest.raises(SandboxError, match=r"Unsupported Daytona GPU type: A100, T4\. Supported types: H100, H200"):
            daytona_gpu_type(["A100", "T4"])


class TestModalGpuType:
    """Modal names its GPUs in capitals, so a spelling is normalized and the first type Modal lists is asked for."""

    @pytest.mark.parametrize(
        ("name", "native"),
        [
            ("h100", "H100"),
            ("NVIDIA H100 80GB", "H100"),
            ("a100", "A100"),
            ("A100-80GB", "A100-80GB"),
            ("nvidia-l40s", "L40S"),
            ("RTX PRO 6000", "RTX-PRO-6000"),
            ("b300", "B300"),
        ],
    )
    def test_a_name_in_any_common_form_maps_to_modals_type(self, name: str, native: str) -> None:
        assert modal_gpu_type([name]) == native

    def test_a_name_modal_does_not_list_is_passed_through_so_new_types_keep_working(self) -> None:
        assert modal_gpu_type(["", "B900"]) == "B900"
        assert modal_gpu_type(["rtx-4090", "rtx-5090"]) == "RTX-4090"


class TestResourcesGpuTypes:
    def test_the_acceptable_types_are_the_list_or_the_single_type(self) -> None:
        assert Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_type=["H100", "A100"]).acceptable_gpu_types == [
            "H100",
            "A100",
        ]
        assert Resources(vcpu=1, memory=1, disk=1, gpu=1, gpu_type="H100").acceptable_gpu_types == ["H100"]
        assert Resources(vcpu=1, memory=1, disk=1).acceptable_gpu_types == []
        assert Resources(vcpu=1, memory=1, disk=1, gpu_type=[]).acceptable_gpu_types == []

    def test_a_string_stays_a_string_and_a_list_stays_a_list_through_json(self) -> None:
        single = Resources.model_validate_json('{"vcpu": 1, "memory": 1, "disk": 1, "gpu": 1, "gpu_type": "H100"}')
        several = Resources.model_validate_json('{"vcpu": 1, "memory": 1, "disk": 1, "gpu": 1, "gpu_type": ["A100"]}')

        assert (single.gpu_type, several.gpu_type) == ("H100", ["A100"])
        assert (single.model_dump()["gpu_type"], several.model_dump()["gpu_type"]) == ("H100", ["A100"])

    def test_types_without_a_gpu_count_are_refused(self) -> None:
        with pytest.raises(ValueError, match="gpu_type requires gpu >= 1"):
            Resources(vcpu=1, memory=1, disk=1, gpu_type=["H100"])
