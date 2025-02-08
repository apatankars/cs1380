const config = {ip: '127.0.0.1', port: 2345};
const distribution = require('../config.js')(config);
const local = distribution.local;
const routes = distribution.local.routes;

test('(4 pts) local.routes.get(status)', (done) => {
  const status = local.status;

  local.routes.get('status', (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(status);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(4 pts) local.routes.get(routes)', (done) => {
  local.routes.get('routes', (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(routes);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(4 pts) local.routes.get(comm)', (done) => {
  const comm = local.comm;

  local.routes.get('comm', (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(comm);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(3 pts) local.routes.get(random)', (done) => {
  local.routes.get('random', (e, v) => {
    try {
      expect(e).toBeDefined();
      expect(e).toBeInstanceOf(Error);
      expect(v).toBeFalsy();
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(6 pts) local.routes.put/get(echo)', (done) => {
  const echoService = {};

  echoService.echo = () => {
    return 'echo!';
  };

  local.routes.put(echoService, 'echo', (e, v) => {
    local.routes.get('echo', (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v.echo()).toBe('echo!');
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

test('(8 pts) routes: put() -> get()', (done) => {
  const otherService = {};

  otherService.gotcha = () => {
    return 'gotcha!';
  };

  routes.put(otherService, 'other', (e, v) => {
    routes.get('other', (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v.gotcha()).toBe('gotcha!');
        done();
      } catch (error) {
        done(error);
      }
    });
  });
});

test('(3 pts) comm: routes.get()', (done) => {
  const remote = {node: config, service: 'routes', method: 'get'};
  const message = [
    'status',
  ];
  distribution.node.start((server) => {
    local.comm.send(message, remote, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toBeDefined();
        done();
      } catch (error) {
        done(error);
      } finally {
        server.close();
      }
    });
  });
});
