/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

import com.cinchapi.concourse.server.concurrent.Token;

/**
 * A {@link TokenEventAnnouncer} informs
 * {@link #observeVersionChanges(TokenEventObserver) registered observers}
 * about any {@link TokenEvent token events} it facilitates or becomes aware of.
 * <p>
 * All {@link TokenEvent token events} are {@link #announceVersionChange(Token)
 * announced}, so {@link TokenEventObserver observers} are expected to filter
 * those which are relevant based on the concerned {@link Token} and
 * {@link TokenEvent event type}
 * </p>
 *
 * @author Jeff Nelson
 */
interface TokenEventAnnouncer {

    /**
     * Announce a version change for the {@code tokens} to all
     * {@link TokenEventObserver observers}.
     * 
     * @param token
     */
    public default void announce(Token... tokens) {
        announce(TokenEvent.VERSION_CHANGE, tokens);
    }

    /**
     * Announce the occurrence of {@code event} for the {@code tokens} to all
     * {@link TokenEventObserver observers}.
     * 
     * @param event
     * @param token
     */
    public void announce(TokenEvent event, Token... tokens);

    /**
     * Add {@code observer} to the list of those that receive {@link TokenEvent}
     * {@link #announceVersionChange(Token) announcements}.
     * 
     * @param observer
     */
    public void subscribe(TokenEventObserver observer);

    /**
     * Remove {@code observer} from the list of those that receive
     * {@link TokenEvent} {@link #announceVersionChange(Token) announcements}.
     * 
     * @param observer
     */
    public void unsubscribe(TokenEventObserver observer);

}
