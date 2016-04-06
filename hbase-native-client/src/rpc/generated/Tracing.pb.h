// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Tracing.proto

#ifndef PROTOBUF_Tracing_2eproto__INCLUDED
#define PROTOBUF_Tracing_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2005000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2005000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/unknown_field_set.h>
// @@protoc_insertion_point(includes)

namespace hbase {
namespace pb {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_Tracing_2eproto();
void protobuf_AssignDesc_Tracing_2eproto();
void protobuf_ShutdownFile_Tracing_2eproto();

class RPCTInfo;

// ===================================================================

class RPCTInfo : public ::google::protobuf::Message {
 public:
  RPCTInfo();
  virtual ~RPCTInfo();

  RPCTInfo(const RPCTInfo& from);

  inline RPCTInfo& operator=(const RPCTInfo& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const RPCTInfo& default_instance();

  void Swap(RPCTInfo* other);

  // implements Message ----------------------------------------------

  RPCTInfo* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const RPCTInfo& from);
  void MergeFrom(const RPCTInfo& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional int64 trace_id = 1;
  inline bool has_trace_id() const;
  inline void clear_trace_id();
  static const int kTraceIdFieldNumber = 1;
  inline ::google::protobuf::int64 trace_id() const;
  inline void set_trace_id(::google::protobuf::int64 value);

  // optional int64 parent_id = 2;
  inline bool has_parent_id() const;
  inline void clear_parent_id();
  static const int kParentIdFieldNumber = 2;
  inline ::google::protobuf::int64 parent_id() const;
  inline void set_parent_id(::google::protobuf::int64 value);

  // @@protoc_insertion_point(class_scope:hbase.pb.RPCTInfo)
 private:
  inline void set_has_trace_id();
  inline void clear_has_trace_id();
  inline void set_has_parent_id();
  inline void clear_has_parent_id();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::int64 trace_id_;
  ::google::protobuf::int64 parent_id_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(2 + 31) / 32];

  friend void  protobuf_AddDesc_Tracing_2eproto();
  friend void protobuf_AssignDesc_Tracing_2eproto();
  friend void protobuf_ShutdownFile_Tracing_2eproto();

  void InitAsDefaultInstance();
  static RPCTInfo* default_instance_;
};
// ===================================================================


// ===================================================================

// RPCTInfo

// optional int64 trace_id = 1;
inline bool RPCTInfo::has_trace_id() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void RPCTInfo::set_has_trace_id() {
  _has_bits_[0] |= 0x00000001u;
}
inline void RPCTInfo::clear_has_trace_id() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void RPCTInfo::clear_trace_id() {
  trace_id_ = GOOGLE_LONGLONG(0);
  clear_has_trace_id();
}
inline ::google::protobuf::int64 RPCTInfo::trace_id() const {
  return trace_id_;
}
inline void RPCTInfo::set_trace_id(::google::protobuf::int64 value) {
  set_has_trace_id();
  trace_id_ = value;
}

// optional int64 parent_id = 2;
inline bool RPCTInfo::has_parent_id() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void RPCTInfo::set_has_parent_id() {
  _has_bits_[0] |= 0x00000002u;
}
inline void RPCTInfo::clear_has_parent_id() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void RPCTInfo::clear_parent_id() {
  parent_id_ = GOOGLE_LONGLONG(0);
  clear_has_parent_id();
}
inline ::google::protobuf::int64 RPCTInfo::parent_id() const {
  return parent_id_;
}
inline void RPCTInfo::set_parent_id(::google::protobuf::int64 value) {
  set_has_parent_id();
  parent_id_ = value;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace pb
}  // namespace hbase

#ifndef SWIG
namespace google {
namespace protobuf {


}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_Tracing_2eproto__INCLUDED