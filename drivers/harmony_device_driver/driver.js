'use strict';

const Homey = require('homey');

class HarmonyDeviceDriver extends Homey.Driver {

    async onInit() {
        console.log('Harmony device driver initializing...');
    }

    async onPair(session) {
        const state = {
            connected: true,
            hub: undefined
        };

        session.setHandler('select_hub', async (data) => {
            this.homey.app.findHubs();
            await new Promise(resolve => setTimeout(resolve, 3000));

            const result = [];
            const hubs = this.homey.app.getHubs();
            hubs.forEach(function(hub) {
                result.push({
                    id: hub.uuid,
                    name: hub.friendlyName,
                    icon: hub.icon
                })
            }, this);

            return result;
        });

        session.setHandler('hub_changed', async (data) => {
            state.hub = this.homey.app.getHub(data.logitech_hubId);
        });

        session.setHandler('manual_hub', async (data) => {
            if (!data || !data.ip) {
                throw new Error('No IP address provided');
            }

            const hub = await this.homey.app.discoverHubByIp(data.ip);
            state.hub = hub;

            return {
                id: hub.uuid || `manual-${hub.ip}`,
                name: hub.friendlyName || `Harmony Hub ${hub.ip}`,
                icon: hub.icon || `/app/${Homey.manifest.id}/assets/icon.svg`
            };
        });

        session.setHandler('list_devices', async (data) => {
            console.log('DeviceDriver: List devices started...');
            return this.homey.app.getHubDevices(state.hub.ip, state.hub.uuid).then((devices) => {
                return (devices);
            });

        })
    }

}

module.exports = HarmonyDeviceDriver;
