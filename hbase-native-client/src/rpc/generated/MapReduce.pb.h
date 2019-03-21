// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: MapReduce.proto

#ifndef PROTOBUF_MapReduce_2eproto__INCLUDED
#define PROTOBUF_MapReduce_2eproto__INCLUDED

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
#include "HBase.pb.h"
// @@protoc_insertion_point(includes)

namespace hbase {
namespace pb {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_MapReduce_2eproto();
void protobuf_AssignDesc_MapReduce_2eproto();
void protobuf_ShutdownFile_MapReduce_2eproto();

class ScanMetrics;
class TableSnapshotRegionSplit;

// ===================================================================

class ScanMetrics : public ::google::protobuf::Message {
 public:
  ScanMetrics();
  virtual ~ScanMetrics();

  ScanMetrics(const ScanMetrics& from);

  inline ScanMetrics& operator=(const ScanMetrics& from) {
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
  static const ScanMetrics& default_instance();

  void Swap(ScanMetrics* other);

  // implements Message ----------------------------------------------

  ScanMetrics* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const ScanMetrics& from);
  void MergeFrom(const ScanMetrics& from);
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

  // repeated .hbase.pb.NameInt64Pair metrics = 1;
  inline int metrics_size() const;
  inline void clear_metrics();
  static const int kMetricsFieldNumber = 1;
  inline const ::hbase::pb::NameInt64Pair& metrics(int index) const;
  inline ::hbase::pb::NameInt64Pair* mutable_metrics(int index);
  inline ::hbase::pb::NameInt64Pair* add_metrics();
  inline const ::google::protobuf::RepeatedPtrField< ::hbase::pb::NameInt64Pair >&
      metrics() const;
  inline ::google::protobuf::RepeatedPtrField< ::hbase::pb::NameInt64Pair >*
      mutable_metrics();

  // @@protoc_insertion_point(class_scope:hbase.pb.ScanMetrics)
 private:

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::RepeatedPtrField< ::hbase::pb::NameInt64Pair > metrics_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(1 + 31) / 32];

  friend void  protobuf_AddDesc_MapReduce_2eproto();
  friend void protobuf_AssignDesc_MapReduce_2eproto();
  friend void protobuf_ShutdownFile_MapReduce_2eproto();

  void InitAsDefaultInstance();
  static ScanMetrics* default_instance_;
};
// -------------------------------------------------------------------

class TableSnapshotRegionSplit : public ::google::protobuf::Message {
 public:
  TableSnapshotRegionSplit();
  virtual ~TableSnapshotRegionSplit();

  TableSnapshotRegionSplit(const TableSnapshotRegionSplit& from);

  inline TableSnapshotRegionSplit& operator=(const TableSnapshotRegionSplit& from) {
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
  static const TableSnapshotRegionSplit& default_instance();

  void Swap(TableSnapshotRegionSplit* other);

  // implements Message ----------------------------------------------

  TableSnapshotRegionSplit* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const TableSnapshotRegionSplit& from);
  void MergeFrom(const TableSnapshotRegionSplit& from);
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

  // repeated string locations = 2;
  inline int locations_size() const;
  inline void clear_locations();
  static const int kLocationsFieldNumber = 2;
  inline const ::std::string& locations(int index) const;
  inline ::std::string* mutable_locations(int index);
  inline void set_locations(int index, const ::std::string& value);
  inline void set_locations(int index, const char* value);
  inline void set_locations(int index, const char* value, size_t size);
  inline ::std::string* add_locations();
  inline void add_locations(const ::std::string& value);
  inline void add_locations(const char* value);
  inline void add_locations(const char* value, size_t size);
  inline const ::google::protobuf::RepeatedPtrField< ::std::string>& locations() const;
  inline ::google::protobuf::RepeatedPtrField< ::std::string>* mutable_locations();

  // optional .hbase.pb.TableSchema table = 3;
  inline bool has_table() const;
  inline void clear_table();
  static const int kTableFieldNumber = 3;
  inline const ::hbase::pb::TableSchema& table() const;
  inline ::hbase::pb::TableSchema* mutable_table();
  inline ::hbase::pb::TableSchema* release_table();
  inline void set_allocated_table(::hbase::pb::TableSchema* table);

  // optional .hbase.pb.RegionInfo region = 4;
  inline bool has_region() const;
  inline void clear_region();
  static const int kRegionFieldNumber = 4;
  inline const ::hbase::pb::RegionInfo& region() const;
  inline ::hbase::pb::RegionInfo* mutable_region();
  inline ::hbase::pb::RegionInfo* release_region();
  inline void set_allocated_region(::hbase::pb::RegionInfo* region);

  // @@protoc_insertion_point(class_scope:hbase.pb.TableSnapshotRegionSplit)
 private:
  inline void set_has_table();
  inline void clear_has_table();
  inline void set_has_region();
  inline void clear_has_region();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::RepeatedPtrField< ::std::string> locations_;
  ::hbase::pb::TableSchema* table_;
  ::hbase::pb::RegionInfo* region_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(3 + 31) / 32];

  friend void  protobuf_AddDesc_MapReduce_2eproto();
  friend void protobuf_AssignDesc_MapReduce_2eproto();
  friend void protobuf_ShutdownFile_MapReduce_2eproto();

  void InitAsDefaultInstance();
  static TableSnapshotRegionSplit* default_instance_;
};
// ===================================================================


// ===================================================================

// ScanMetrics

// repeated .hbase.pb.NameInt64Pair metrics = 1;
inline int ScanMetrics::metrics_size() const {
  return metrics_.size();
}
inline void ScanMetrics::clear_metrics() {
  metrics_.Clear();
}
inline const ::hbase::pb::NameInt64Pair& ScanMetrics::metrics(int index) const {
  return metrics_.Get(index);
}
inline ::hbase::pb::NameInt64Pair* ScanMetrics::mutable_metrics(int index) {
  return metrics_.Mutable(index);
}
inline ::hbase::pb::NameInt64Pair* ScanMetrics::add_metrics() {
  return metrics_.Add();
}
inline const ::google::protobuf::RepeatedPtrField< ::hbase::pb::NameInt64Pair >&
ScanMetrics::metrics() const {
  return metrics_;
}
inline ::google::protobuf::RepeatedPtrField< ::hbase::pb::NameInt64Pair >*
ScanMetrics::mutable_metrics() {
  return &metrics_;
}

// -------------------------------------------------------------------

// TableSnapshotRegionSplit

// repeated string locations = 2;
inline int TableSnapshotRegionSplit::locations_size() const {
  return locations_.size();
}
inline void TableSnapshotRegionSplit::clear_locations() {
  locations_.Clear();
}
inline const ::std::string& TableSnapshotRegionSplit::locations(int index) const {
  return locations_.Get(index);
}
inline ::std::string* TableSnapshotRegionSplit::mutable_locations(int index) {
  return locations_.Mutable(index);
}
inline void TableSnapshotRegionSplit::set_locations(int index, const ::std::string& value) {
  locations_.Mutable(index)->assign(value);
}
inline void TableSnapshotRegionSplit::set_locations(int index, const char* value) {
  locations_.Mutable(index)->assign(value);
}
inline void TableSnapshotRegionSplit::set_locations(int index, const char* value, size_t size) {
  locations_.Mutable(index)->assign(
    reinterpret_cast<const char*>(value), size);
}
inline ::std::string* TableSnapshotRegionSplit::add_locations() {
  return locations_.Add();
}
inline void TableSnapshotRegionSplit::add_locations(const ::std::string& value) {
  locations_.Add()->assign(value);
}
inline void TableSnapshotRegionSplit::add_locations(const char* value) {
  locations_.Add()->assign(value);
}
inline void TableSnapshotRegionSplit::add_locations(const char* value, size_t size) {
  locations_.Add()->assign(reinterpret_cast<const char*>(value), size);
}
inline const ::google::protobuf::RepeatedPtrField< ::std::string>&
TableSnapshotRegionSplit::locations() const {
  return locations_;
}
inline ::google::protobuf::RepeatedPtrField< ::std::string>*
TableSnapshotRegionSplit::mutable_locations() {
  return &locations_;
}

// optional .hbase.pb.TableSchema table = 3;
inline bool TableSnapshotRegionSplit::has_table() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void TableSnapshotRegionSplit::set_has_table() {
  _has_bits_[0] |= 0x00000002u;
}
inline void TableSnapshotRegionSplit::clear_has_table() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void TableSnapshotRegionSplit::clear_table() {
  if (table_ != NULL) table_->::hbase::pb::TableSchema::Clear();
  clear_has_table();
}
inline const ::hbase::pb::TableSchema& TableSnapshotRegionSplit::table() const {
  return table_ != NULL ? *table_ : *default_instance_->table_;
}
inline ::hbase::pb::TableSchema* TableSnapshotRegionSplit::mutable_table() {
  set_has_table();
  if (table_ == NULL) table_ = new ::hbase::pb::TableSchema;
  return table_;
}
inline ::hbase::pb::TableSchema* TableSnapshotRegionSplit::release_table() {
  clear_has_table();
  ::hbase::pb::TableSchema* temp = table_;
  table_ = NULL;
  return temp;
}
inline void TableSnapshotRegionSplit::set_allocated_table(::hbase::pb::TableSchema* table) {
  delete table_;
  table_ = table;
  if (table) {
    set_has_table();
  } else {
    clear_has_table();
  }
}

// optional .hbase.pb.RegionInfo region = 4;
inline bool TableSnapshotRegionSplit::has_region() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void TableSnapshotRegionSplit::set_has_region() {
  _has_bits_[0] |= 0x00000004u;
}
inline void TableSnapshotRegionSplit::clear_has_region() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void TableSnapshotRegionSplit::clear_region() {
  if (region_ != NULL) region_->::hbase::pb::RegionInfo::Clear();
  clear_has_region();
}
inline const ::hbase::pb::RegionInfo& TableSnapshotRegionSplit::region() const {
  return region_ != NULL ? *region_ : *default_instance_->region_;
}
inline ::hbase::pb::RegionInfo* TableSnapshotRegionSplit::mutable_region() {
  set_has_region();
  if (region_ == NULL) region_ = new ::hbase::pb::RegionInfo;
  return region_;
}
inline ::hbase::pb::RegionInfo* TableSnapshotRegionSplit::release_region() {
  clear_has_region();
  ::hbase::pb::RegionInfo* temp = region_;
  region_ = NULL;
  return temp;
}
inline void TableSnapshotRegionSplit::set_allocated_region(::hbase::pb::RegionInfo* region) {
  delete region_;
  region_ = region;
  if (region) {
    set_has_region();
  } else {
    clear_has_region();
  }
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

#endif  // PROTOBUF_MapReduce_2eproto__INCLUDED