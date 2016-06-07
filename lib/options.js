var extend = require('deep-extend');

module.exports = function (userOptions, defaultOptions, mandatoryOptions) {
  var hasMandatoryOptions = true;
  mandatoryOptions.forEach(function (opt) {
    if (!userOptions[opt]) {
      console.error(`Method call must have the "${opt}" option defined!`);
      hasMandatoryOptions = false;
    }
  });

  if (!hasMandatoryOptions) throw new Error('missing_options');
  return extend({}, defaultOptions, userOptions);
}
