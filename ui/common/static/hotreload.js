(function () {
  var lastUuid = "";
  var timeout;

  const resetBackoff = () => {
    timeout = 1000;
  };

  const backOff = () => {
    if (timeout > 10 * 1000) {
      return;
    }

    timeout = timeout * 2;
  };

  const hotReloadUrl = () => {
    const hostAndPort =
      location.hostname + (location.port ? ":" + location.port : "");

    if (location.protocol === "https:") {
      return "wss://" + hostAndPort + "/ws/hotreload";
    } else if (location.protocol === "http:") {
      return "ws://" + hostAndPort + "/ws/hotreload";
    }
  };

  function connectHotReload() {
    const socket = new WebSocket(hotReloadUrl());

    socket.onmessage = (event) => {
      if (lastUuid === "") {
        lastUuid = event.data;
      }

      if (lastUuid !== event.data) {
        console.log("[Hot Reloader] Server Changed, reloading");
        location.reload();
      }
    };

    socket.onopen = () => {
      resetBackoff();
      socket.send("Hello");
    };

    socket.onclose = () => {
      const timeoutId = setTimeout(function () {
        clearTimeout(timeoutId);
        backOff();

        connectHotReload();
      }, timeout);
    };
  }

  resetBackoff();
  connectHotReload();
})();
