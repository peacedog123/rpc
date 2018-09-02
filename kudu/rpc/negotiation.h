#pragma once

#include <iosfwd>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"

namespace kudu {

class MonoTime;

namespace rpc {

class Connection;

class Negotiation {
 public:

  // Perform negotiation for a connection (either server or client)
  static void RunNegotiation(const scoped_refptr<Connection>& conn,
                             MonoTime deadline);
 private:
  DISALLOW_IMPLICIT_CONSTRUCTORS(Negotiation);
};

}}
