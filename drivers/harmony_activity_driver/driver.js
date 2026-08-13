'use strict';

const Homey = require('homey');

class HarmonyActivityDriver extends Homey.Driver {

    async onInit() {
        console.log('Harmony activity driver initializing...');
    }

    async onPair(session) {
        console.log('Harmony activity driver: onPair started');

        session.setHandler('list_devices', async () => {
            console.log('ActivityDriver: list_devices started');

            const hub = await this.homey.app.resolvePairingHub();
            if (!hub || !hub.ip) {
                throw new Error('No Harmony Hub found.');
            }

            console.log(`ActivityDriver: listing activities for ${hub.ip}`);
            return this.homey.app.getHubActivities(hub.ip, hub.uuid);
        });
    }

}

module.exports = HarmonyActivityDriver;
