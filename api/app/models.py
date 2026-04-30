from datetime import datetime, timezone
from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


class MeasurementIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    device_eui: str = Field(..., min_length=1)
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    co2_ppm: int = Field(..., ge=0)
    temp_c: Optional[float] = Field(default=None, validation_alias=AliasChoices("temp_c", "temperature_c"))
    rh: Optional[float] = Field(default=None, validation_alias=AliasChoices("rh", "humidity_rh"))
    battery_v: Optional[float] = None
    rssi: Optional[int] = Field(default=None, validation_alias=AliasChoices("rssi", "rssi_dbm"))
    snr: Optional[float] = Field(default=None, validation_alias=AliasChoices("snr", "snr_db"))
    firmware_version: Optional[str] = None
    gateway_eui: Optional[str] = None
    gateway_name: Optional[str] = None
    organization_name: str = "Demo"
    site_name: str = "Main Site"
    room_name: str = "Unassigned"
    device_name: Optional[str] = None
    battery_type: Optional[str] = None
    target_co2_ppm: Optional[int] = Field(default=1000, ge=400)

    @model_validator(mode="after")
    def normalize_timestamp(self):
        if self.ts.tzinfo is None:
            self.ts = self.ts.replace(tzinfo=timezone.utc)
        return self
