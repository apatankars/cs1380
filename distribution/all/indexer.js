const indexer = function(config) {
  const context = {};
  context.gid = config.gid || 'all';

  cb = (error, value) => {
    if (error) {
      console.error(error);
    } else {
      console.log(value);
    }
  }



  return {
    /**
     * 
     * @param {*} configuration the configuration should contain the 
     *  {
     *      key: this should be the name of the page (exclude the -wiki prefix)
     *      value: this should be the object that is store
     *  }
     * @param {*} callback 
     */
    index: (configuration, callback) => {

    }
    
  };
};

module.exports = indexer;
