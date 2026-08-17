'use strict';
const inspector = require('inspector');
const Homey = require('homey');
const HubManager = require('./lib/hubmanager.js');
const Discovery = require('./lib/discovery.js');
const CapabilityHelper = require('./lib/capabilityhelper.js');
const { getCanonicalHubId, normalizeHubInfo, deviceEventKey } = require('./lib/hubidentity.js');

const capabilityhelper = new CapabilityHelper();

const iconsMap = {
    AirConditioner: 'Air Conditioner.svg',
    Amplifier: 'Amplifier.svg',
    AppleTV: 'Apple TV.svg',
    AudioVideoSwitch: 'Audio Video Switch.svg',
    AutomationGateway: 'Automation gateway.svg',
    Blinds: 'Blinds.svg',
    MiniSystemCDRadioCassette: 'Boombox.svg',
    MiniSystemDvdCDRadio: 'Boombox.svg',
    MiniSystemDvdVcrRadio: 'Boombox.svg',
    Camera: 'Camera.svg',
    CDPlayer: 'CD Player.svg',
    TVDVD: 'Clasic Television.svg',
    TVDVDVCR: 'Clasic Television.svg',
    TVHDD: 'Clasic Television.svg',
    TVVCR: 'Clasic Television.svg',
    ClimateControl: 'Climate Control.svg',
    Computer: 'Computer.svg',
    DAT: 'Digital Audio Cassette.svg',
    Dimmer: 'Dimmer.svg',
    DoorLock: 'Doorlock.svg',
    DVDRVCR: 'DVD Player.svg',
    DVDRecorder: 'DVD Player.svg',
    DVDVCR: 'DVD Player.svg',
    DVD: 'DVD.svg',
    Fan: 'Fan.svg',
    GameConsole: 'Game Console.svg',
    GameConsoleWithDvd: 'Game Console.svg',
    StereoReceiver: 'Hi-Fi Stereo.svg',
    HomeAppliance: 'Home Appliances.svg',
    Controller: 'Home Automation.svg',
    CDJukebox: 'Jukebox.svg',
    Laptop: 'Laptop.svg',
    LaserdiscPlayer: 'Laser Disc.svg',
    LightController: 'Light controller.svg',
    MediaPlayer: 'Media Player.svg',
    MinidiscPlayer: 'Mini disk.svg',
    Monitor: 'Monitor.svg',
    DigitalMusicServer: 'Music server.svg',
    Plug: 'Plug.svg',
    ProjectorScreen: 'Projector Screen.svg',
    Projector: 'Projector.svg',
    RadioTuner: 'Radio.svg',
    PVR: 'PVR.svg',
    Satellite: 'Satelite Dish.svg',
    Sensor: 'Sensor.svg',
    CableBox: 'Set-top box.svg',
    DigitalSetTopBox: 'Set-top box.svg',
    SmokeDetector: 'Smoke Detector.svg',
    Television: 'Television.svg',
    Thermostat: 'Thermostat.svg',
    Nest: 'Thermostat.svg',
    VCR: 'VCR.svg',
    TVCamera: 'Video Camera.svg'
}

class App extends Homey.App {

    async onInit() {
        this.log(`${Homey.manifest.id} running (pairing via app settings IP)...`);

        Homey.app = this.homey.app;

        if (process.env.DEBUG === '1')
            inspector.open(8080, '127.0.0.1', true)

        this._hubs = [];
        this._hubManager = new HubManager(this.homey);
        this._discover = new Discovery(this._hubManager, this.homey);
        this._discover.start();

        this.wireEvents();
        this.registerActions();
        this._registerSettingsListener();
        await this._connectConfiguredHub();
    }

    _readConfiguredIp(value) {
        return typeof value === 'string' ? value.trim() : '';
    }

    _registerSettingsListener() {
        this.homey.settings.on('set', (key) => {
            if (key !== 'harmonyHubIp')
                return;

            this._connectConfiguredHub().catch((err) => {
                this.error(`Failed to apply Harmony Hub IP setting: ${err}`);
            });
        });
    }

    async _connectConfiguredHub() {
        const ip = this._readConfiguredIp(await this.homey.settings.get('harmonyHubIp'));
        if (!ip)
            return;

        this.log(`Probing configured hub IP ${ip}`);
        try {
            await this.discoverHubByIp(ip);
        } catch (err) {
            this.error(`Configured hub IP ${ip} is not reachable: ${err.message}`);
        }
    }

    wireEvents() {
        this._discover.on('hubconnected', hub => {
            const normalized = normalizeHubInfo(hub);
            if (!getCanonicalHubId(normalized) || !normalized.ip || !normalized.friendlyName)
                return;

            this.addHub(normalized);
            this.emit('hubonline', normalized, true);
        });

        this._hubManager.on('activityChanged', (activityName, hubId) => {
            this.log(activityName);
            const foundHub = this.getHub(hubId);

            if (foundHub === undefined)
                return;

            const tokens = {
                hub: foundHub.friendlyName,
                activity: activityName
            }

            const activityStartedTrigger = this.homey.flow.getTriggerCard('activity_started');
            activityStartedTrigger.trigger(tokens).catch((err) => this.error(err));
        });

        this._hubManager.on('inactivitytime', (minutes, hubId) => {
            const state = { inactivefor: minutes }
            const foundHub = this.getHub(hubId);

            if (foundHub === undefined)
                return;

            const tokens = {
                hub: foundHub.friendlyName
            }

            const inactiveTrigger = this.homey.flow.getTriggerCard('hub_inactive')
            inactiveTrigger.trigger(tokens, state).catch((err) => this.error(err));
        })

        this._hubManager.on('activityChanging', (activityName, hubId) => {
            this.log(activityName);
            const foundHub = this.getHub(hubId);

            if (foundHub === undefined)
                return;

            const tokens = {
                hub: foundHub.friendlyName,
                activity: activityName
            }

            this._activityStartingTrigger = this.homey.flow.getTriggerCard('activity_starting');
            this._activityStartingTrigger.trigger(tokens).catch((err) => this.error(err));

        });

        this._hubManager.on('activityStopped', (activityName, hubId) => {
            const foundHub = this.getHub(hubId);

            if (foundHub === undefined)
                return;

            const tokens = {
                hub: foundHub.friendlyName,
                activity: activityName
            }

            this._activityStoppedTrigger = this.homey.flow.getTriggerCard('activity_stopped');

            this._activityStoppedTrigger.trigger(tokens).catch((err) => this.error(err));
        });
    }

    findHubs() {
        this.log('Finding hubs....')
        this._discover.start();
    }

    discoverHubByIp(ip) {
        const trimmedIp = typeof ip === 'string' ? ip.trim() : ip;
        this.log(`App: discoverHubByIp ${trimmedIp}`);
        return this._discover.discoverHubByIp(trimmedIp);
    }

    async resolvePairingHub() {
        const ip = this._readConfiguredIp(await this.homey.settings.get('harmonyHubIp'));

        if (ip) {
            this.log(`App: probing configured hub IP ${ip}`);
            try {
                return await this.discoverHubByIp(ip);
            } catch (err) {
                throw new Error(`Could not reach Harmony Hub at ${ip}: ${err.message}`);
            }
        }

        this.findHubs();
        const hubs = this.getHubs();
        if (hubs.length === 0)
            throw new Error(
                'No Harmony Hub IP configured. Open App settings, enter the hub IP under Harmony Hub, save, and try pairing again.'
            );

        const hub = hubs[0];
        if (hub.remoteId)
            return hub;

        return this.discoverHubByIp(hub.ip);
    }

    addHub(hub) {
        const normalized = normalizeHubInfo(hub);
        const id = getCanonicalHubId(normalized);
        if (!normalized.friendlyName || !normalized.ip || !id)
            return undefined;

        const existing = this.getHub(id) || this._hubs.find(item => item.ip === normalized.ip);
        if (existing) {
            existing.ip = normalized.ip;
            existing.friendlyName = normalized.friendlyName || existing.friendlyName;
            existing.remoteId = normalized.remoteId || existing.remoteId;
            existing.hubId = normalized.hubId || existing.hubId;
            existing.uuid = normalized.uuid || existing.uuid;
            return existing;
        }

        normalized.icon = `/app/${Homey.manifest.id}/assets/icon.svg`;
        this._hubs.push(normalized);
        this.log(`discovered ${normalized.ip} ${normalized.friendlyName} ${id}`);
        return normalized;
    }

    getHub(hubId) {
        const id = hubId != null ? String(hubId) : '';
        const foundHub = this._hubs.find(x =>
            String(x.remoteId) === id || String(x.hubId) === id || String(x.uuid) === id
        );

        if (foundHub === undefined)
            this.log(`No hub found with id ${hubId}`)

        return foundHub;
    }

    getHubs() {
        return this._hubs;
    }

    _waitUntilHubReady(hub, timeoutMs) {
        const timeout = timeoutMs || 8000;
        if (hub.hubConnection && hub.hubConnection.readyState === 1 && hub.activities.length)
            return Promise.resolve(hub);

        return new Promise((resolve, reject) => {
            const started = Date.now();
            const timer = this.homey.setInterval(() => {
                const connected = hub.hubConnection && hub.hubConnection.readyState === 1;
                if (connected && (hub.devices.length || hub.activities.length)) {
                    this.homey.clearInterval(timer);
                    return resolve(hub);
                }
                if (Date.now() - started > timeout) {
                    this.homey.clearInterval(timer);
                    if (connected)
                        return resolve(hub);
                    reject(new Error('Hub did not connect in time'));
                }
            }, 200);
        });
    }

    async getHubActivities(ip, hubId) {
        const hub = await this._hubManager.connectToHub(ip);
        if (!hub)
            throw new Error('Hub is not connected');

        await this._waitUntilHubReady(hub);

        const canonicalHubId = getCanonicalHubId(hub) || hubId;
        const activities = [];

        for (const activity of hub.activities) {
            if (activity.controlGroup === undefined)
                continue;

            const capabilities = await capabilityhelper.getCapabilities(activity.controlGroup);
            capabilities.push('onoff');

            const foundDevice = {
                name: activity.label,
                capabilities,
                data: {
                    id: deviceEventKey(canonicalHubId, activity.id),
                    hubId: String(canonicalHubId),
                    harmonyId: String(activity.id),
                    controlGroup: activity.controlGroup,
                    label: activity.label
                }
            };

            if (activity.type === 'VirtualTelevisionN')
                foundDevice.class = 'tv';

            activities.push(foundDevice);
        }

        return activities;
    }

    async getHubDevices(ip, hubId) {
        const hub = await this._hubManager.connectToHub(ip);
        if (!hub)
            throw new Error('Hub is not connected');

        await this._waitUntilHubReady(hub);

        const canonicalHubId = getCanonicalHubId(hub) || hubId;
        const devices = [];

        for (const device of hub.devices) {
            const capabilities = await capabilityhelper.getCapabilities(device.controlGroup);
            this.log(device.type);
            const iconName = iconsMap[device.type];
            const iconPath = iconName !== undefined ? `/device_icons/${iconName}` : '/icon.svg';

            devices.push({
                name: device.label,
                icon: iconPath,
                capabilities,
                data: {
                    id: deviceEventKey(canonicalHubId, device.id),
                    hubId: String(canonicalHubId),
                    harmonyId: String(device.id),
                    controlGroup: device.controlGroup,
                    label: device.label
                }
            });
        }

        return devices;
    }

    registerActions() {
        const sendCommandAction = this.homey.flow.getActionCard('send_command');

        this.controlGroupAutoComplete(sendCommandAction);
        this.commandAutocomplete(sendCommandAction);
        this.registerSendCommandRunListener(sendCommandAction);

        const startActivityAction = this.homey.flow.getActionCard('start_activity');
        this.hubAutoComplete(startActivityAction);
        this.activityAutoComplete(startActivityAction);
        this.registerStartActivityCommandRunListener(startActivityAction);

        const stopActivityAction = this.homey.flow.getActionCard('stop_activity');
        this.hubAutoComplete(stopActivityAction);
        this.registerStopActivityCommandRunListener(stopActivityAction);

        const isActivityCondition = this.homey.flow
            .getConditionCard('is_activity')
            .registerRunListener((args, state) => {
                this.log(args.activity_input);
                this.log(args.activity.name)
                const isActivity = args.activity_input.trim() === args.activity.name.trim();
                this.log(isActivity);
                return Promise.resolve(isActivity);
            });
        this.hubAutoComplete(isActivityCondition);
        this.activityAutoComplete(isActivityCondition);

        this.homey.flow.getTriggerCard('hub_inactive')
            .registerRunListener((args, state) => {
                return Promise.resolve(Number(state.inactivefor) === Number(args.inactivefor));
            })

    }

    registerStopActivityCommandRunListener(stopActivityAction) {
        stopActivityAction
            .registerRunListener((args, state) => {
                this.log('Stop activity!!');
                const hubArgValue = args.hub;
                const hubId = hubArgValue.hubId;
                const foundHub = this.getHub(hubId);

                return new Promise((resolve, reject) => {
                    if (foundHub === undefined)
                        return reject();

                    this._hubManager.connectToHub(foundHub.ip).then((hub) => {
                        if (!hub)
                            return reject(new Error('Hub connection not found'));

                        hub.stopActivity().then(() => {
                            resolve();
                        }).catch((err) => {
                            this.error(err);
                            reject(err);
                        });
                    }).catch((err) => {
                        this.error(err);
                        reject(err);
                    });
                });
            })
    }

    registerStartActivityCommandRunListener(startActivityAction) {
        startActivityAction
            .registerRunListener((args, state) => {
                this.log('Start activity!!');
                const hubArgValue = args.hub;
                const hubId = hubArgValue.hubId;
                const activityId = args.activity.activityId;
                const foundHub = this.getHub(hubId);

                return new Promise((resolve, reject) => {
                    if (foundHub === undefined)
                        return reject();

                    this._hubManager.connectToHub(foundHub.ip).then((hub) => {
                        if (!hub)
                            return reject(new Error('Hub connection not found'));

                        hub.startActivity(activityId).then(() => {
                            resolve();
                        }).catch((err) => {
                            this.error(err);
                            reject(err);
                        });
                    }).catch((err) => {
                        reject(err);
                    });
                });
            })
    }

    hubAutoComplete(startActivityAction) {
        startActivityAction
            .getArgument('hub')
            .registerAutocompleteListener((query, args) => {
                const result = [];
                this._hubs.forEach((hub) => {
                    const autocompleteItem = {
                        name: hub.friendlyName,
                        hubId: getCanonicalHubId(hub)
                    };
                    result.push(autocompleteItem);
                });

                return Promise.resolve(result);
            })
    }

    activityAutoComplete(startActivityAction) {
        startActivityAction
            .getArgument('activity')
            .registerAutocompleteListener((query, args) => {
                return new Promise((resolve, reject) => {
                    const result = [];
                    const hubArgValue = args.hub;
                    if (!hubArgValue)
                        return resolve(result);

                    const foundHub = this.getHub(hubArgValue.hubId);

                    if (foundHub === undefined)
                        return reject();

                    this._hubManager.connectToHub(foundHub.ip).then((hub) => {
                        if (!hub)
                            return resolve(result);

                        hub.activities.forEach((activity) => {
                            const autocompleteItem = {
                                name: activity.label,
                                activityId: activity.id
                            };
                            result.push(autocompleteItem);
                        });
                        resolve(result);
                    }).catch(reject);

                });
            });
    }

    registerSendCommandRunListener(sendCommandAction) {
        sendCommandAction
            .registerRunListener((args, state) => {
                this.log('Send Command!!');
                const hubDevice = args.device;
                const hubDeviceData = hubDevice.getData();
                const hubId = hubDeviceData.hubId;
                const foundHub = this.getHub(hubId);
                const controlCommandArgValue = args.control_command;
                const repeat = args.control_command_repeat;

                return new Promise((resolve, reject) => {
                    if (foundHub === undefined)
                        return reject();

                    for (let index = 0; index - 1 < repeat; index++) {
                        this._hubManager.connectToHub(foundHub.ip).then((hub) => {
                            if (!hub)
                                return;

                            hub.commandAction(controlCommandArgValue.command).catch((err) => {
                                this.error(err);
                                reject(err);
                            });
                        }).catch((err) => {
                            this.error(err);
                        });

                        if (index === repeat)
                            resolve(true);
                    }
                });
            })
    }

    commandAutocomplete(sendCommandAction) {
        sendCommandAction
            .getArgument('control_command')
            .registerAutocompleteListener((query, args) => {
                const hubDevice = args.device;
                const hubDeviceData = hubDevice.getData();
                const controlGroupArgValue = args.control_group;
                const result = [];

                if (controlGroupArgValue !== '') {
                    const controlGroup = hubDeviceData.controlGroup.find(x => x.name === controlGroupArgValue.name);

                    if (controlGroup !== undefined)
                        controlGroup.function.forEach((command) => {
                            const autocompleteItem = {
                                name: command.label,
                                command
                            };
                            result.push(autocompleteItem);
                        });

                }
                return Promise.resolve(result);
            })
    }

    controlGroupAutoComplete(sendCommandAction) {
        sendCommandAction
            .getArgument('control_group')
            .registerAutocompleteListener((query, args) => {
                const hubDevice = args.device;
                const hubDeviceData = hubDevice.getData();
                const result = [];

                hubDeviceData.controlGroup.forEach((group) => {
                    const autocompleteItem = {
                        name: group.name
                    };
                    result.push(autocompleteItem);
                });

                return Promise.resolve(result);
            })
    }

    getPairedDevices() {
        this.log('getPairedDevices...');
        const summarize = (device) => {
            const data = device.getData();
            return {
                name: device.getName(),
                id: data.id,
                hubId: data.hubId,
                harmonyId: data.harmonyId,
                available: device.getAvailable()
            };
        };

        const deviceDriver = this.homey.drivers.getDriver('harmony_device_driver');
        const activityDriver = this.homey.drivers.getDriver('harmony_activity_driver');

        return {
            devices: deviceDriver.getDevices().map(summarize),
            activities: activityDriver.getDevices().map(summarize),
            hubs: this._hubs.map((hub) => ({
                ip: hub.ip,
                friendlyName: hub.friendlyName,
                remoteId: hub.remoteId
            }))
        };
    }

    sendDebugReport() {
        const report = this.getPairedDevices();
        this.log('Diagnostic report generated locally');
        return report;
    }

}

module.exports = App;
