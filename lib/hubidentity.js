'use strict';

function asId(value) {
    if (value == null || value === '')
        return undefined;
    return String(value);
}

function getCanonicalHubId(hub) {
    if (!hub)
        return undefined;
    return asId(hub.remoteId) || asId(hub.hubId) || asId(hub.uuid);
}

function normalizeHubInfo(hubInfo) {
    if (!hubInfo)
        return {};

    const normalized = Object.assign({}, hubInfo);
    const remoteId = asId(hubInfo.remoteId) || asId(hubInfo.hubId);

    if (remoteId) {
        normalized.remoteId = remoteId;
        normalized.hubId = remoteId;
        normalized.uuid = remoteId;
    } else if (hubInfo.uuid != null)
        normalized.uuid = asId(hubInfo.uuid);

    if (typeof normalized.ip === 'string')
        normalized.ip = normalized.ip.trim();

    return normalized;
}

function deviceEventKey(hubId, harmonyId) {
    return `${asId(hubId)}:${asId(harmonyId)}`;
}

function sameHubId(left, right) {
    const a = asId(left);
    const b = asId(right);
    return a != null && a === b;
}

module.exports = {
    asId,
    getCanonicalHubId,
    normalizeHubInfo,
    deviceEventKey,
    sameHubId
};
