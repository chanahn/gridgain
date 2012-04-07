// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Ssl {
    using System;
    using System.Net.Sockets;
    using System.Net.Security;
    using System.Security.Cryptography.X509Certificates;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * This class provides basic initialization of the SSL stream for
     * client-server communication.</summary>
     */
    public class GridClientSslContext : IGridClientSslContext {
        /** <summary>Allow all server certificates callback.</summary> */
        public static readonly RemoteCertificateValidationCallback AllowAllCerts = (i1, i2, i3, i4) => true;

        /** <summary>Deny all server certificates callback.</summary> */
        public static readonly RemoteCertificateValidationCallback DenyAllCerts = (i1, i2, i3, i4) => false;

        /** <summary>Validate certificates chain user-defined callback.</summary> */
        private RemoteCertificateValidationCallback callback;

        /** <summary>Constructs default context, which ignores any SSL errors.</summary> */
        public GridClientSslContext()
            : this(true) {
        }

        /**
         * <summary>
         * Constructs SSL context with permanent validation result.</summary>
         *
         * <param name="permanentResult">Permanent validation result: allow all certificates or deny all.</param>
         */
        public GridClientSslContext(bool permanentResult) {
            ValidateCallback = permanentResult ? AllowAllCerts : DenyAllCerts;
        }

        /** <summary>Validate certificates chain user-defined callback.</summary> */
        public RemoteCertificateValidationCallback ValidateCallback {
            get {
                return callback;
            }
            set {
                A.NotNull(value, "ValidateCallback");

                callback = value;
            }
        }

        /**
         * <summary>
         * Constructs SSL stream for the client.</summary>
         *
         * <param name="client">Tcp client for client-server communication.</param>
         * <returns>Configured SSL stream.</returns>
         */
        public SslStream CreateStream(TcpClient client) {
            return new SslStream(client.GetStream(), false, callback, null);
        }
    }
}
