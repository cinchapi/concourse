/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
 * A recipient of {@link TokenEventAnnouncer#announce(TokenEvent, Token)
 * announcements} about {@link TokenEvent token events}.
 *
 * @author Jeff Nelson
 */
interface TokenEventObserver {

    /**
     * Handle the announcement of {@code event} for {@code token}.
     * <p>
     * This method will observe announcements for all {@link TokenEvent events}
     * concerning all {@link Token tokens}, so filtering logic must be
     * implemented here, if it is required.
     * </p>
     * 
     * @param event
     * @param token
     * @return {@code true} if observing {@code event} for {@code token} has any
     *         side effect.
     */
    public boolean observe(TokenEvent event, Token token);

}
