'use strict';
class CapabilityHelper {

    getCapabilities(controlGroup) {
        const capabilities = [];

        if (!Array.isArray(controlGroup))
            return Promise.resolve(capabilities);

        controlGroup.forEach((group) => {
            if (group.name === 'Power')
                capabilities.push('onoff');

            if (group.name === 'Home' && Array.isArray(group.function))
                group.function.forEach((command) => {
                    if (command.name === 'PowerOn')
                        capabilities.push('onoff');

                    if (command.name === 'PowerToggle')
                        capabilities.push('onoff');

                });

            if (group.name === 'Volume' && Array.isArray(group.function))
                group.function.forEach((command) => {
                    if (command.name === 'Mute')
                        capabilities.push('volume_mute');

                    if (command.name === 'VolumeDown')
                        capabilities.push('volume_down');

                    if (command.name === 'VolumeUp')
                        capabilities.push('volume_up');

                });

            if (group.name === 'Channel' && Array.isArray(group.function))
                group.function.forEach((command) => {
                    if (command.name === 'ChannelDown')
                        capabilities.push('channel_down');

                    if (command.name === 'ChannelUp')
                        capabilities.push('channel_up');

                });

        });

        return Promise.resolve(capabilities);
    }

}
module.exports = CapabilityHelper;
