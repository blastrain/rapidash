<template>
  <v-container fluid row>
    <v-layout row wrap>
      <v-flex sm12 md9 offset-lg1 lg9>
        <div
          style="position:relative; top: 150px; overflow: scroll;"
          :style="{ height: (contentHeight - 100) + 'px' }"
        >
          <div class="log-group" v-for="(commands, index) in commandGroups" :key="index">
            <span class="label-transaction-id">{{ commands[0].id }}</span>
            <rapidash-log
              v-for="(command, index) in commands"
              :key="index"
              :log="command"
              :num="index"
              :widthMap="widthMap"
              :class="{ odd: index % 2 === 1, even: index % 2 === 0}"
              @changedValue="changedValue"
            ></rapidash-log>
          </div>
          <div style="position:relative; height:150px;"></div>
        </div>
        <v-layout style="position:relative; top: -100%; height:0px;">
          <v-flex md1>
            <rapidash-fetch-point ref="app" :areaHeight="height" name="app"></rapidash-fetch-point>
          </v-flex>
          <v-flex md1 offset-md2>
            <rapidash-fetch-point ref="stash" :areaHeight="height" name="stash"></rapidash-fetch-point>
          </v-flex>
          <v-flex md1 offset-md2>
            <rapidash-fetch-point ref="server" :areaHeight="height" name="server"></rapidash-fetch-point>
          </v-flex>
          <v-flex md1 offset-md2>
            <rapidash-fetch-point ref="db" :areaHeight="height" name="db"></rapidash-fetch-point>
          </v-flex>
        </v-layout>
      </v-flex>
      <v-flex hidden-sm-and-down md3 lg2>
        <v-layout
          class="value-viewer ml-4"
          :style="{ height: (contentHeight - 300) + 'px' }"
          align-start
          wrap
        >
          <code>{{ value }}</code>
        </v-layout>
      </v-flex>
    </v-layout>
  </v-container>
</template>

<style>
.log-group {
  position: relative;
  left: 2px;
  width: 95%;
  min-width: 700px;
  border-radius: 5px;
  margin-top: 10px;
  margin-bottom: 10px;
  padding: 10px;
  background-color: white;
  box-shadow: 0px 0px 3px 1px gray;
}
.even {
  background-color: #e6e6fa;
}
.odd {
  background-color: #faf0e6;
}
.label-transaction-id {
  position: relative;
  vertical-align: top;
  top: -5px;
  left: -8px;
  font-size: 14px;
  padding: 4px;
  border-radius: 5px;
  background-color: #6a5acd;
  color: white;
}
.value-viewer {
  position: relative;
  border-radius: 5px;
  top: 160px;
  overflow: scroll;
  box-shadow: 0px 0px 3px 1px gray;
}
code {
  position: relative;
  width: 100%;
  height: 100%;
}
</style>


<script>
import Vue from "vue";
import Vuetify from "vuetify";
import FetchPoint from "./components/FetchPoint.vue";
import Log from "./components/Log.vue";

Vue.use(Vuetify);
Vue.component("rapidash-fetch-point", FetchPoint);
Vue.component("rapidash-log", Log);

export default {
  data: function() {
    return {
      height: window.innerHeight - 52,
      contentHeight: window.innerHeight,
      value: "",
      widthMap: {
        app: '0px',
        server: '0px',
        stash: '0px',
        db: '0px',
      }
    };
  },
  computed: {
    commandGroups: function () {
      const commands = window.logs;
      const commandGroups = {};
      const commandGroupIds = [];
      commands.forEach(command => {
        if (!commandGroups[command.id]) {
          commandGroups[command.id] = [command];
          commandGroupIds.push(command.id);
        } else {
          commandGroups[command.id].push(command);
        }
      });
      return commandGroupIds.map(id => {
        return commandGroups[id];
      });
    },
  },
  methods: {
    changedValue: function(log) {
      if (!log.value) {
        this.value = "no content";
        return;
      }
      const value = log.value;
      this.value = value
        .replace(/(,)([^{)])/g, ",\n\t$2")
        .replace(/{/g, "{\n\t")
        .replace(/}/g, "\n  }");
    },
    handleResize: function(e) {
      this.height = window.innerHeight - 52;
      this.contentHeight = window.innerHeight;

      this.setWidthMap();
    },
    setWidthMap: function() {
      this.widthMap.app = this.getWidth('app');
      this.widthMap.stash = this.getWidth('stash');
      this.widthMap.server = this.getWidth('server');
      this.widthMap.db = this.getWidth('db');
    },
    getWidth: function(type) {
      if (type === 'app') {
        return this.$refs[type].$el.getBoundingClientRect().left;
      }
      const fetchPointX = this.$refs[type].$el.getBoundingClientRect().left;
      const width = fetchPointX - this.$refs.app.$el.getBoundingClientRect().left - 10;
      return `${width}px`;
    }
  },
  mounted: function() {
    this.setWidthMap();
    window.addEventListener("resize", this.handleResize);
  },
  beforeDestroy: function() {
    window.removeEventListener("resize", this.handleResize);
  }
};
</script>
