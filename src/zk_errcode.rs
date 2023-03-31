use num_derive::{FromPrimitive, ToPrimitive};

#[derive(Debug, FromPrimitive, ToPrimitive)]
pub enum ZooErrors {
    // Everything is OK */
    ZOK = 0,

    // System and server-side errors. This is never thrown by the server,
    // it shouldn't be used other than to indicate a range.
    // Specifically error codes greater than this value, but lesser than ZAPIERROR, are system errors.
    ZSYSTEMERROR = -1,
    // A runtime inconsistency was found */
    ZRUNTIMEINCONSISTENCY = -2,
    // A data inconsistency was found */
    ZDATAINCONSISTENCY = -3,
    // Connection to the server has been lost */
    ZCONNECTIONLOSS = -4,
    // Error while marshalling or unmarshalling data */
    ZMARSHALLINGERROR = -5,
    // Operation is unimplemented */
    ZUNIMPLEMENTED = -6,
    // Operation timeout */
    ZOPERATIONTIMEOUT = -7,
    ZBADARGUMENTS = -8,
    // Invalid arguments */
    ZINVALIDSTATE = -9,
    // Invliad zhandle state */
    ZNEWCONFIGNOQUORUM = -13,
    // No quorum of new config is connected and up-to-date with the leader of last commmitted config - try invoking reconfiguration after new servers are connected and synced */
    ZRECONFIGINPROGRESS = -14,
    // Reconfiguration requested while another reconfiguration is currently in progress. This is currently not supported. Please retry. */
    ZSSLCONNECTIONERROR = -15, // The SSL connection Error */

    // API errors.
    // This is never thrown by the server, it shouldn't be used other than
    // to indicate a range. Specifically error codes greater than this
    // value are API errors (while values less than this indicate a ZSYSTEMERROR.
    ZAPIERROR = -100,
    ZNONODE = -101,
    // Node does not exist */
    ZNOAUTH = -102,
    // Not authenticated */
    ZBADVERSION = -103,
    // Version conflict */
    ZNOCHILDRENFOREPHEMERALS = -108,
    // Ephemeral nodes may not have children */
    ZNODEEXISTS = -110,
    // The node already exists */
    ZNOTEMPTY = -111,
    // The node has children */
    ZSESSIONEXPIRED = -112,
    // The session has been expired by the server */
    ZINVALIDCALLBACK = -113,
    // Invalid callback specified */
    // Invalid ACL specified */
    ZINVALIDACL = -114,
    // Client authentication failed */
    ZAUTHFAILED = -115,
    // ZooKeeper is closing */
    ZCLOSING = -116,
    // (not error) no server responses to process */
    ZNOTHING = -117,
    ZSESSIONMOVED = -118,
    // !<session moved to another server, so operation is ignored */
    ZNOTREADONLY = -119,
    // state-changing request is passed to read-only server */
    ZEPHEMERALONLOCALSESSION = -120,
    // Attempt to create ephemeral node on a local session */
    ZNOWATCHER = -121,
    // The watcher couldn't be found */
    ZRECONFIGDISABLED = -123,
    // Attempts to perform a reconfiguration operation when reconfiguration feature is disabled */
    ZSESSIONCLOSEDREQUIRESASLAUTH = -124,
    // The session has been closed by server because server requires client to do authentication via configured authentication scheme at server, but client is not configured with required authentication scheme or configured but failed (i.e. wrong credential used.). */
    ZTHROTTLEDOP = -127, // Operation was throttled and not executed at all. please, retry! */

                         /* when adding/changing values here also update zerror(int) to return correct error message */
}
