"""Dynamic Protobuf decoder without requiring external protoc compiler."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.json_format import MessageToDict

from eventlens.decoders.base import BaseDecoder, DecodedPayload

FIELD_TYPE_MAP: dict[str, int] = {
    "double": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    "float": descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT,
    "int64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "uint64": descriptor_pb2.FieldDescriptorProto.TYPE_UINT64,
    "int32": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
    "fixed64": descriptor_pb2.FieldDescriptorProto.TYPE_FIXED64,
    "fixed32": descriptor_pb2.FieldDescriptorProto.TYPE_FIXED32,
    "bool": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "bytes": descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    "uint32": descriptor_pb2.FieldDescriptorProto.TYPE_UINT32,
    "sfixed32": descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED32,
    "sfixed64": descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED64,
    "sint32": descriptor_pb2.FieldDescriptorProto.TYPE_SINT32,
    "sint64": descriptor_pb2.FieldDescriptorProto.TYPE_SINT64,
}


class ProtoParser:
    """Parses .proto schema definitions into FileDescriptorProto."""

    @classmethod
    def parse_proto_content(cls, content: str, filename: str = "dynamic.proto") -> descriptor_pb2.FileDescriptorProto:
        file_proto = descriptor_pb2.FileDescriptorProto()
        file_proto.name = filename

        # Strip line comments
        cleaned_lines: list[str] = []
        for line in content.splitlines():
            line = re.sub(r"//.*$", "", line).strip()
            if line:
                cleaned_lines.append(line)
        text = " ".join(cleaned_lines)

        # Match package
        pkg_match = re.search(r"package\s+([\w\.]+)\s*;", text)
        package = pkg_match.group(1) if pkg_match else ""
        if package:
            file_proto.package = package

        # Find all message definitions
        message_pattern = re.compile(r"message\s+(\w+)\s*\{([^}]+)\}")
        for match in message_pattern.finditer(text):
            msg_name = match.group(1)
            body = match.group(2)

            msg_proto = file_proto.message_type.add()
            msg_proto.name = msg_name

            # Parse fields inside message body
            field_pattern = re.compile(r"(repeated|optional)?\s*([\w\.]+)\s+(\w+)\s*=\s*(\d+)\s*;")
            for f_match in field_pattern.finditer(body):
                modifier = f_match.group(1)
                type_name = f_match.group(2)
                f_name = f_match.group(3)
                f_number = int(f_match.group(4))

                field = msg_proto.field.add()
                field.name = f_name
                field.number = f_number

                if modifier == "repeated":
                    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
                else:
                    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

                if type_name in FIELD_TYPE_MAP:
                    field.type = FIELD_TYPE_MAP[type_name]
                else:
                    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
                    full_type_name = f".{package}.{type_name}" if package else f".{type_name}"
                    field.type_name = full_type_name

        return file_proto


class ProtoDecoder(BaseDecoder):
    """Decodes binary Protobuf payloads into dictionaries."""

    def __init__(
        self,
        proto_file_path: str | Path | None = None,
        proto_content: str | None = None,
        message_type: str | None = None,
    ) -> None:
        self.message_type = message_type
        self.pool = descriptor_pool.DescriptorPool()
        self.message_classes: dict[str, Any] = {}
        self.has_schema = False

        if proto_file_path or proto_content:
            self.load_schema(proto_file_path=proto_file_path, proto_content=proto_content)

    @property
    def name(self) -> str:
        return "protobuf"

    def load_schema(
        self,
        proto_file_path: str | Path | None = None,
        proto_content: str | None = None,
    ) -> None:
        """Load and register proto definitions."""
        if proto_file_path:
            p = Path(proto_file_path)
            if not p.exists():
                raise FileNotFoundError(f"Proto file not found: {p}")
            content = p.read_text(encoding="utf-8")
            filename = p.name
        elif proto_content:
            content = proto_content
            filename = "inline.proto"
        else:
            return

        file_proto = ProtoParser.parse_proto_content(content, filename=filename)
        self.pool.Add(file_proto)

        package_prefix = f"{file_proto.package}." if file_proto.package else ""
        for msg_type in file_proto.message_type:
            full_name = f"{package_prefix}{msg_type.name}"
            try:
                desc = self.pool.FindMessageTypeByName(full_name)
                cls = message_factory.GetMessageClass(desc)
                self.message_classes[msg_type.name] = cls
                self.message_classes[full_name] = cls
                if not self.message_type:
                    self.message_type = msg_type.name
            except Exception:
                continue

        self.has_schema = bool(self.message_classes)

    def can_decode(self, raw: bytes, headers: dict[str, str] | None = None) -> bool:
        """Check if message matches Protobuf indicators."""
        if not raw:
            return False

        # Header hints
        if headers:
            ct = headers.get("content-type", "") or headers.get("Content-Type", "")
            if "protobuf" in ct.lower():
                return True

        if self.has_schema:
            # Attempt trial parsing with registered message class
            cls = self._get_target_class()
            if cls:
                try:
                    msg = cls()
                    msg.ParseFromString(raw)
                    return True
                except Exception:
                    return False

        # Without a registered schema or protobuf header, do not treat raw bytes as protobuf
        return False

    def _get_target_class(self) -> Any:
        if not self.message_classes:
            return None
        if self.message_type and self.message_type in self.message_classes:
            return self.message_classes[self.message_type]
        # Return the first registered class
        return next(iter(self.message_classes.values()), None)

    def decode(self, raw: bytes, headers: dict[str, str] | None = None) -> DecodedPayload:
        """Parse raw bytes into dictionary."""
        cls = self._get_target_class()
        if cls:
            try:
                msg = cls()
                msg.ParseFromString(raw)
                data = MessageToDict(
                    msg,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                return DecodedPayload(
                    format=self.name,
                    data=data,
                    raw_bytes=raw,
                    is_structured=True,
                )
            except Exception as err:
                return DecodedPayload(
                    format=self.name,
                    data=None,
                    raw_bytes=raw,
                    is_structured=False,
                    error=f"Protobuf schema decode error: {err}",
                )

        # Fallback: Schemaless raw wire parsing
        try:
            wire_data = self._parse_schemaless_wire(raw)
            return DecodedPayload(
                format=self.name,
                data=wire_data,
                raw_bytes=raw,
                is_structured=True,
            )
        except Exception as err:
            return DecodedPayload(
                format=self.name,
                data=None,
                raw_bytes=raw,
                is_structured=False,
                error=f"Schemaless wire parse error: {err}",
            )

    def _heuristic_wire_check(self, raw: bytes) -> bool:
        """Check if bytes match valid Protobuf varint tag patterns."""
        if len(raw) < 2:
            return False
        first_byte = raw[0]
        wire_type = first_byte & 0x07
        field_num = first_byte >> 3
        return field_num > 0 and wire_type in (0, 1, 2, 5)

    def _parse_schemaless_wire(self, raw: bytes) -> dict[str, Any]:
        """Parse raw Protobuf bytes into generic field-tag dictionary."""
        idx = 0
        length = len(raw)
        result: dict[str, Any] = {}

        while idx < length:
            tag, idx = self._read_varint(raw, idx)
            wire_type = tag & 0x07
            field_num = tag >> 3

            if wire_type == 0:  # Varint
                val, idx = self._read_varint(raw, idx)
                key = f"tag_{field_num}"
                self._append_value(result, key, val)
            elif wire_type == 2:  # Length-delimited (string, bytes, embedded msg)
                val_len, idx = self._read_varint(raw, idx)
                val_bytes = raw[idx : idx + val_len]
                idx += val_len

                # Try UTF-8 string, else recursive wire or hex
                try:
                    text = val_bytes.decode("utf-8")
                    val: Any = text
                except UnicodeDecodeError:
                    val = f"0x{val_bytes.hex()}"

                key = f"tag_{field_num}"
                self._append_value(result, key, val)
            elif wire_type == 1:  # 64-bit
                idx += 8
                self._append_value(result, f"tag_{field_num}", "fixed64")
            elif wire_type == 5:  # 32-bit
                idx += 4
                self._append_value(result, f"tag_{field_num}", "fixed32")
            else:
                break

        return result

    @staticmethod
    def _read_varint(buffer: bytes, offset: int) -> tuple[int, int]:
        result = 0
        shift = 0
        while offset < len(buffer):
            b = buffer[offset]
            offset += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result, offset
            shift += 7
        return result, offset

    @staticmethod
    def _append_value(target: dict[str, Any], key: str, value: Any) -> None:
        if key in target:
            existing = target[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                target[key] = [existing, value]
        else:
            target[key] = value
