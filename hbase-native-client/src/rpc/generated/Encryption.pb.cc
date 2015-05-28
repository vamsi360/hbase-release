// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Encryption.proto

#define INTERNAL_SUPPRESS_PROTOBUF_FIELD_DEPRECATION
#include "Encryption.pb.h"

#include <algorithm>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/once.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)

namespace hbase {
namespace pb {

namespace {

const ::google::protobuf::Descriptor* WrappedKey_descriptor_ = NULL;
const ::google::protobuf::internal::GeneratedMessageReflection*
  WrappedKey_reflection_ = NULL;

}  // namespace


void protobuf_AssignDesc_Encryption_2eproto() {
  protobuf_AddDesc_Encryption_2eproto();
  const ::google::protobuf::FileDescriptor* file =
    ::google::protobuf::DescriptorPool::generated_pool()->FindFileByName(
      "Encryption.proto");
  GOOGLE_CHECK(file != NULL);
  WrappedKey_descriptor_ = file->message_type(0);
  static const int WrappedKey_offsets_[5] = {
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, algorithm_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, length_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, data_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, iv_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, hash_),
  };
  WrappedKey_reflection_ =
    new ::google::protobuf::internal::GeneratedMessageReflection(
      WrappedKey_descriptor_,
      WrappedKey::default_instance_,
      WrappedKey_offsets_,
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, _has_bits_[0]),
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(WrappedKey, _unknown_fields_),
      -1,
      ::google::protobuf::DescriptorPool::generated_pool(),
      ::google::protobuf::MessageFactory::generated_factory(),
      sizeof(WrappedKey));
}

namespace {

GOOGLE_PROTOBUF_DECLARE_ONCE(protobuf_AssignDescriptors_once_);
inline void protobuf_AssignDescriptorsOnce() {
  ::google::protobuf::GoogleOnceInit(&protobuf_AssignDescriptors_once_,
                 &protobuf_AssignDesc_Encryption_2eproto);
}

void protobuf_RegisterTypes(const ::std::string&) {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedMessage(
    WrappedKey_descriptor_, &WrappedKey::default_instance());
}

}  // namespace

void protobuf_ShutdownFile_Encryption_2eproto() {
  delete WrappedKey::default_instance_;
  delete WrappedKey_reflection_;
}

void protobuf_AddDesc_Encryption_2eproto() {
  static bool already_here = false;
  if (already_here) return;
  already_here = true;
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  ::google::protobuf::DescriptorPool::InternalAddGeneratedFile(
    "\n\020Encryption.proto\022\010hbase.pb\"W\n\nWrappedK"
    "ey\022\021\n\talgorithm\030\001 \002(\t\022\016\n\006length\030\002 \002(\r\022\014\n"
    "\004data\030\003 \002(\014\022\n\n\002iv\030\004 \001(\014\022\014\n\004hash\030\005 \001(\014BC\n"
    "*org.apache.hadoop.hbase.protobuf.genera"
    "tedB\020EncryptionProtosH\001\240\001\001", 186);
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedFile(
    "Encryption.proto", &protobuf_RegisterTypes);
  WrappedKey::default_instance_ = new WrappedKey();
  WrappedKey::default_instance_->InitAsDefaultInstance();
  ::google::protobuf::internal::OnShutdown(&protobuf_ShutdownFile_Encryption_2eproto);
}

// Force AddDescriptors() to be called at static initialization time.
struct StaticDescriptorInitializer_Encryption_2eproto {
  StaticDescriptorInitializer_Encryption_2eproto() {
    protobuf_AddDesc_Encryption_2eproto();
  }
} static_descriptor_initializer_Encryption_2eproto_;

// ===================================================================

#ifndef _MSC_VER
const int WrappedKey::kAlgorithmFieldNumber;
const int WrappedKey::kLengthFieldNumber;
const int WrappedKey::kDataFieldNumber;
const int WrappedKey::kIvFieldNumber;
const int WrappedKey::kHashFieldNumber;
#endif  // !_MSC_VER

WrappedKey::WrappedKey()
  : ::google::protobuf::Message() {
  SharedCtor();
}

void WrappedKey::InitAsDefaultInstance() {
}

WrappedKey::WrappedKey(const WrappedKey& from)
  : ::google::protobuf::Message() {
  SharedCtor();
  MergeFrom(from);
}

void WrappedKey::SharedCtor() {
  _cached_size_ = 0;
  algorithm_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  length_ = 0u;
  data_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  iv_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  hash_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
}

WrappedKey::~WrappedKey() {
  SharedDtor();
}

void WrappedKey::SharedDtor() {
  if (algorithm_ != &::google::protobuf::internal::kEmptyString) {
    delete algorithm_;
  }
  if (data_ != &::google::protobuf::internal::kEmptyString) {
    delete data_;
  }
  if (iv_ != &::google::protobuf::internal::kEmptyString) {
    delete iv_;
  }
  if (hash_ != &::google::protobuf::internal::kEmptyString) {
    delete hash_;
  }
  if (this != default_instance_) {
  }
}

void WrappedKey::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* WrappedKey::descriptor() {
  protobuf_AssignDescriptorsOnce();
  return WrappedKey_descriptor_;
}

const WrappedKey& WrappedKey::default_instance() {
  if (default_instance_ == NULL) protobuf_AddDesc_Encryption_2eproto();
  return *default_instance_;
}

WrappedKey* WrappedKey::default_instance_ = NULL;

WrappedKey* WrappedKey::New() const {
  return new WrappedKey;
}

void WrappedKey::Clear() {
  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (has_algorithm()) {
      if (algorithm_ != &::google::protobuf::internal::kEmptyString) {
        algorithm_->clear();
      }
    }
    length_ = 0u;
    if (has_data()) {
      if (data_ != &::google::protobuf::internal::kEmptyString) {
        data_->clear();
      }
    }
    if (has_iv()) {
      if (iv_ != &::google::protobuf::internal::kEmptyString) {
        iv_->clear();
      }
    }
    if (has_hash()) {
      if (hash_ != &::google::protobuf::internal::kEmptyString) {
        hash_->clear();
      }
    }
  }
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
  mutable_unknown_fields()->Clear();
}

bool WrappedKey::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!(EXPRESSION)) return false
  ::google::protobuf::uint32 tag;
  while ((tag = input->ReadTag()) != 0) {
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // required string algorithm = 1;
      case 1: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_algorithm()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->algorithm().data(), this->algorithm().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(16)) goto parse_length;
        break;
      }

      // required uint32 length = 2;
      case 2: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_VARINT) {
         parse_length:
          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   ::google::protobuf::uint32, ::google::protobuf::internal::WireFormatLite::TYPE_UINT32>(
                 input, &length_)));
          set_has_length();
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(26)) goto parse_data;
        break;
      }

      // required bytes data = 3;
      case 3: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_data:
          DO_(::google::protobuf::internal::WireFormatLite::ReadBytes(
                input, this->mutable_data()));
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(34)) goto parse_iv;
        break;
      }

      // optional bytes iv = 4;
      case 4: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_iv:
          DO_(::google::protobuf::internal::WireFormatLite::ReadBytes(
                input, this->mutable_iv()));
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(42)) goto parse_hash;
        break;
      }

      // optional bytes hash = 5;
      case 5: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_hash:
          DO_(::google::protobuf::internal::WireFormatLite::ReadBytes(
                input, this->mutable_hash()));
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectAtEnd()) return true;
        break;
      }

      default: {
      handle_uninterpreted:
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_END_GROUP) {
          return true;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, mutable_unknown_fields()));
        break;
      }
    }
  }
  return true;
#undef DO_
}

void WrappedKey::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // required string algorithm = 1;
  if (has_algorithm()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->algorithm().data(), this->algorithm().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      1, this->algorithm(), output);
  }

  // required uint32 length = 2;
  if (has_length()) {
    ::google::protobuf::internal::WireFormatLite::WriteUInt32(2, this->length(), output);
  }

  // required bytes data = 3;
  if (has_data()) {
    ::google::protobuf::internal::WireFormatLite::WriteBytes(
      3, this->data(), output);
  }

  // optional bytes iv = 4;
  if (has_iv()) {
    ::google::protobuf::internal::WireFormatLite::WriteBytes(
      4, this->iv(), output);
  }

  // optional bytes hash = 5;
  if (has_hash()) {
    ::google::protobuf::internal::WireFormatLite::WriteBytes(
      5, this->hash(), output);
  }

  if (!unknown_fields().empty()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        unknown_fields(), output);
  }
}

::google::protobuf::uint8* WrappedKey::SerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // required string algorithm = 1;
  if (has_algorithm()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->algorithm().data(), this->algorithm().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        1, this->algorithm(), target);
  }

  // required uint32 length = 2;
  if (has_length()) {
    target = ::google::protobuf::internal::WireFormatLite::WriteUInt32ToArray(2, this->length(), target);
  }

  // required bytes data = 3;
  if (has_data()) {
    target =
      ::google::protobuf::internal::WireFormatLite::WriteBytesToArray(
        3, this->data(), target);
  }

  // optional bytes iv = 4;
  if (has_iv()) {
    target =
      ::google::protobuf::internal::WireFormatLite::WriteBytesToArray(
        4, this->iv(), target);
  }

  // optional bytes hash = 5;
  if (has_hash()) {
    target =
      ::google::protobuf::internal::WireFormatLite::WriteBytesToArray(
        5, this->hash(), target);
  }

  if (!unknown_fields().empty()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        unknown_fields(), target);
  }
  return target;
}

int WrappedKey::ByteSize() const {
  int total_size = 0;

  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    // required string algorithm = 1;
    if (has_algorithm()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->algorithm());
    }

    // required uint32 length = 2;
    if (has_length()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::UInt32Size(
          this->length());
    }

    // required bytes data = 3;
    if (has_data()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::BytesSize(
          this->data());
    }

    // optional bytes iv = 4;
    if (has_iv()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::BytesSize(
          this->iv());
    }

    // optional bytes hash = 5;
    if (has_hash()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::BytesSize(
          this->hash());
    }

  }
  if (!unknown_fields().empty()) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        unknown_fields());
  }
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = total_size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
  return total_size;
}

void WrappedKey::MergeFrom(const ::google::protobuf::Message& from) {
  GOOGLE_CHECK_NE(&from, this);
  const WrappedKey* source =
    ::google::protobuf::internal::dynamic_cast_if_available<const WrappedKey*>(
      &from);
  if (source == NULL) {
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
    MergeFrom(*source);
  }
}

void WrappedKey::MergeFrom(const WrappedKey& from) {
  GOOGLE_CHECK_NE(&from, this);
  if (from._has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (from.has_algorithm()) {
      set_algorithm(from.algorithm());
    }
    if (from.has_length()) {
      set_length(from.length());
    }
    if (from.has_data()) {
      set_data(from.data());
    }
    if (from.has_iv()) {
      set_iv(from.iv());
    }
    if (from.has_hash()) {
      set_hash(from.hash());
    }
  }
  mutable_unknown_fields()->MergeFrom(from.unknown_fields());
}

void WrappedKey::CopyFrom(const ::google::protobuf::Message& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void WrappedKey::CopyFrom(const WrappedKey& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool WrappedKey::IsInitialized() const {
  if ((_has_bits_[0] & 0x00000007) != 0x00000007) return false;

  return true;
}

void WrappedKey::Swap(WrappedKey* other) {
  if (other != this) {
    std::swap(algorithm_, other->algorithm_);
    std::swap(length_, other->length_);
    std::swap(data_, other->data_);
    std::swap(iv_, other->iv_);
    std::swap(hash_, other->hash_);
    std::swap(_has_bits_[0], other->_has_bits_[0]);
    _unknown_fields_.Swap(&other->_unknown_fields_);
    std::swap(_cached_size_, other->_cached_size_);
  }
}

::google::protobuf::Metadata WrappedKey::GetMetadata() const {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::Metadata metadata;
  metadata.descriptor = WrappedKey_descriptor_;
  metadata.reflection = WrappedKey_reflection_;
  return metadata;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace pb
}  // namespace hbase

// @@protoc_insertion_point(global_scope)