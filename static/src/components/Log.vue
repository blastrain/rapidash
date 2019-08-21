<template>
  <div class="rapidash-log" @click="click(log)">
    <div class="key" :style="keyStyle">
      <span class="label">
        <span class="label-header">command</span>
        <span
          class="label-body"
          :style="{'background-color': commandColorMap[log.command]}"
        >{{ log.command }}</span>
      </span>
      <span class="label">
        <span class="label-header">table</span>
        <span class="label-body">{{ query.table }}</span>
      </span>
      <span v-for="(index, idx) in query.indexes" :key="idx">
        <span class="label">
          <span
            class="label-index-header"
            :class="{ 'label-index-header-first': idx === 0 }"
          >{{ index.key }}</span>
          <span
            class="label-index-body"
            :class="{ 'label-index-body-last': idx === query.indexes.length - 1}"
          >{{ index.value }}</span>
        </span>
        <span class="label-index-and" v-show="idx !== query.indexes.length - 1">AND</span>
      </span>
      <span
        class="label-body"
        style="background-color: #9932cc;"
        v-show="query.isTransaction"
      >transaction</span>
      <span v-show="log.command === 'get_multi'">...</span>
    </div>
    <div>
      <div class="line" :style="style">
        <div v-show="isGetCommand" class="left-arrow-pos">
          <div class="left-arrow" :style="leftArrowStyle"></div>
        </div>
        <div v-show="!isGetCommand" class="right-arrow-pos" :style="rightArrowPosStyle">
          <div class="right-arrow" :style="rightArrowStyle"></div>
        </div>
      </div>
    </div>
    <div class="box" v-show="isShowValue">{{ log.value }}</div>
  </div>
</template>

<script>
function parseForSelectOrDeleteSQL(query, values) {
  const tableName = query.match(/FROM\s`([\S]+)`/)[1];
  const where = query.match(/WHERE(.+)/)[1];
  const indexes = where.split(/AND/).map(condition => {
    const keyAndValue = condition.split(/=|[IN]/).filter(e => e !== "");
    const baseKey = keyAndValue[0];
    const baseValue = keyAndValue[1].trim();
    const key = baseKey.match(/`([\S]+)`/)[1];
    const argNum = (baseValue.match(/\?/g) || []).length;
    const v = values.splice(0, argNum);
    return {
      key: key,
      value: v.length === 1 ? v[0] : v
    };
  });
  return {
    table: tableName,
    indexes: indexes
  };
}
function parseForInsertSQL(query) {
  const tableName = query.match(/INTO\s`([\S]+)`/)[1];
  return {
    table: tableName
  };
}
function parseForUpdateSQL(query) {
  const tableName = query.match(/UPDATE\s`([\S]+)`/)[1];
  const where = query.match(/WHERE(.+)/)[1];
  const indexes = where.split(/AND/).map(condition => {
    const keyAndValue = condition.split(/=|[IN]/).filter(e => e !== "");
    const key = keyAndValue[0].trim();
    const value = keyAndValue[1].trim();
    return {
      key: key,
      value: value
    };
  });
  return {
    table: tableName,
    indexes: indexes
  };
}
function parseForCacheQuery(queries) {
  const parsedQueries = queries.splice(0, 1).map(query => {
    const elems = query.split("/");
    if (elems[0] !== "r") return {};
    if (elems[1] !== "slc") return {};
    const tableName = elems[2];
    const keyAndValues =
      elems[3] === "uq" || elems[3] === "idx" ? elems[4] : elems[3];
    const indexes = keyAndValues.split("&").map(keyAndValue => {
      const splitted = keyAndValue.split("#");
      const key = splitted[0];
      const value = splitted[1];
      return {
        key: key,
        value: value
      };
    });
    return {
      table: tableName,
      indexes: indexes,
      isTransaction: query.endsWith("tx")
    };
  });
  if (parsedQueries.length === 0) return {};
  return {
    table: parsedQueries[0].table,
    indexes: parsedQueries.map(parsedQuery => parsedQuery.indexes).flat(),
    isTransaction: parsedQueries[0].isTransaction
  };
}

export default {
  props: {
    log: Object,
    num: Number,
    widthMap: Object,
  },
  data: function() {
    const commandColorMap = {
      get: "#3cb371",
      add: "#9932cc",
      set: "#4169e1",
      get_multi: "#da70d6",
      update: "#d2691e",
      delete: "#cd5c5c"
    };
    const lineColor = commandColorMap[this.log.command];
    let parsedQuery = {};
    if (this.log.type === "db") {
      const args = [];
      args.push(this.log.args);
      if (this.log.command === "set") {
        parsedQuery = parseForInsertSQL(this.log.key);
      } else if (this.log.command === "update") {
        parsedQuery = parseForUpdateSQL(this.log.key);
      } else {
        parsedQuery = parseForSelectOrDeleteSQL(this.log.key, args.flat());
      }
    } else if (this.log.key !== "") {
      const queries = [];
      queries.push(this.log.key);
      parsedQuery = parseForCacheQuery(queries.flat());
    }
    return {
      query: parsedQuery,
      commandColorMap: commandColorMap,
      isGetCommand:
        this.log.command === "get" || this.log.command === "get_multi",
      isShowValue: false,
      leftArrowStyle: {
        "border-right": "6px solid " + lineColor
      },
      rightArrowStyle: {
        "border-left": "6px solid " + lineColor
      },
      keyStyle: {
        width: "800px"
      },
      strokeColor: lineColor
    };
  },
  computed: {
    style: function () {
      return {
        fill: this.commandColorMap[this.log.command],
        position: "relative",
        left: "45px",
        width: this.widthMap[this.log.type],
        height: "2px",
        backgroundColor: this.commandColorMap[this.log.command],
      };
    },
    rightArrowPosStyle: function () {
      return {
        left: this.widthMap[this.log.type],
      }
    },
  },
  methods: {
    enter: function(e) {
      this.isShowValue = true;
    },
    leave: function(e) {
      this.isShowValue = false;
    },
    click: function(log) {
      console.log("emit changedValue");
      this.$emit("changedValue", log);
    },
    handleResize: function(e) {
      this.style.width = this.widthMap[this.log.type];
      this.rightArrowPosStyle.left = this.style.width;
    }
  },
  mounted: function() {
    window.addEventListener("resize", this.handleResize);
  },
  beforeDestroy: function() {
    window.removeEventListener("resize", this.handleResize);
  }
};
</script>

<style>
.rapidash-log {
  height: 60px;
  padding-bottom: 10px;
  margin-top: 10px;
  margin-bottom: 10px;
  border-radius: 5px;
}
.left-arrow-pos {
  position: relative;
  top: -5px;
  left: -10px;
}
.right-arrow-pos {
  position: relative;
  top: -5px;
}
.left-arrow {
  left: 6px;
  box-sizing: border-box;
  width: 6px;
  height: 6px;
  border: 6px solid transparent;
  border-right: 6px solid;
}
.right-arrow {
  left: 6px;
  box-sizing: border-box;
  width: 6px;
  height: 6px;
  border: 6px solid transparent;
}
.key {
  position: relative;
  padding-top: 3px;
  top: 10px;
  left: 10px;
  overflow: hidden;
}
.line {
  top: 20px;
}
.box {
  background-color: snow;
  z-index: 1;
}
.label-header {
  position: relative;
  vertical-align: top;
  font-size: 14px;
  padding: 5px;
  top: -2px;
  left: 5px;
  background-color: #696969;
  color: white;
  border-top-left-radius: 7px;
  border-bottom-left-radius: 7px;
}
.label-index-header-first {
  border-top-left-radius: 7px;
  border-bottom-left-radius: 7px;
}
.label-index-header {
  position: relative;
  vertical-align: top;
  font-size: 14px;
  padding: 5px;
  left: 5px;
  top: -2px;
  background-color: #696969;
  color: white;
}
.label-index-and {
  position: relative;
  color: white;
  font-size: 14px;
  padding: 5px;
  margin-left: -4px;
  margin-right: -5px;
  top: -2px;
  vertical-align: top;
  background-color: #d2691e;
}
.label-index-body {
  position: relative;
  color: white;
  padding: 5px;
  font-size: 14px;
  vertical-align: top;
  top: -2px;
  background-color: #6b8e23;
}
.label-index-body-last {
  border-top-right-radius: 7px;
  border-bottom-right-radius: 7px;
}
.label-body {
  position: relative;
  color: white;
  padding: 5px;
  top: -2px;
  font-size: 14px;
  vertical-align: top;
  background-color: #6495ed;
  border-top-right-radius: 7px;
  border-bottom-right-radius: 7px;
}
</style>