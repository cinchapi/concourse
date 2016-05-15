/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.plugin;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.server.plugin.io.SharedMemory;
import com.cinchapi.concourse.thrift.AccessToken;

/**
 * A struct that associates a {@link AccessToken user session} with shared
 * memory segment paths for both incoming and outgoing communication.
 * <p>
 * Concourse Server is responsible for initially creating a {@link PluginClient}
 * and communicating that to the plugin on the broadcast channel. The plugin may
 * also store instances of the {@link PluginClient} locally
 * </p>
 * 
 * @author Jeff Nelson
 */
@Immutable
final class PluginClient implements Serializable {

    /**
     * The serial version UID.
     */
    private static final long serialVersionUID = -7788036321478124640L;

    /**
     * The {@link AccessToken} that represents the user session this object
     * encapsulates.
     */
    public final AccessToken creds;

    /**
     * The path to use for constructing a {@link SharedMemory} segment for
     * incoming (remote -> local) communication.
     */
    public final String inbox;

    /**
     * The path to use for constructing a {@link SharedMemory} segment for
     * outgoing (local -> remote) communication.
     */
    public final String outbox;

    /**
     * A reference to a {@link SharedMemory} instance that is considered the
     * local inbox. This underlying path may be different than {@link #inbox}.
     */
    transient SharedMemory localInbox;

    /**
     * A reference to a {@link SharedMemory} instance that is considered the
     * local outbox. This underlying path may be different than {@link #outbox}.
     */
    transient SharedMemory localOutbox;

    /**
     * Construct a new instance.
     * 
     * @param creds
     * @param inbox
     * @param outbox
     */
    public PluginClient(AccessToken creds, String inbox, String outbox) {
        this.creds = creds;
        this.inbox = inbox;
        this.outbox = outbox;
    }

    @Override
    public int hashCode() {
        return Objects.hash(creds, inbox, outbox);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof PluginClient) {
            PluginClient other = (PluginClient) obj;
            return Objects.equals(creds, other.creds)
                    && Objects.equals(inbox, other.inbox)
                    && Objects.equals(outbox, other.outbox);
        }
        else {
            return false;
        }
    }

}
