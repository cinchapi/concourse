package com.cinchapi.concourse.security;

/**
 * An enum that describes that possible roles for a user.
 *
 * @author Jeff Nelson
 */
public enum Role {
    ADMIN, USER;

    /**
     * Case insensitive implementation of {@link #valueOf(String)}.
     * 
     * @param role
     * @return the parsed Role
     */
    public static Role valueOfIgnoreCase(String role) {
        return Role.valueOf(role.toUpperCase());
    }
}