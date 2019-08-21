
<template>
  <svg viewbox="0 0 300 300" width="100" :height="areaHeight">
    <g :transform="translateTop">
      <ellipse :rx="rx" :ry="ry" fill="white" stroke="black"></ellipse>
    </g>
    <line :x1="50 - rx" :y1="20" :x2="50 - rx" :y2="20 + height" stroke="black"></line>
    <line :x1="50 + rx" :y1="20" :x2="50 + rx" :y2="20 + height" stroke="black"></line>
    <text
      :x="50"
      :y="20 + height / 2"
      text-anchor="middle"
      dominant-baseline="central"
      font-size="14"
    >{{ name }}</text>
    <g :transform="translateBottom">
      <ellipse :rx="rx" :ry="ry" fill="whitesmoke" stroke="black"></ellipse>
    </g>
     <line
        :x1="50"
        :y1="80"
        :x2="50"
        :y2="areaHeight"
        stroke="gray"
        stroke-width="1"
        fill="none"
        stroke-dasharray="10"
      />
  </svg>
</template>

<script>
export default {
  props: {
    x: Number,
    y: Number,
    areaHeight: Number,
    name: String
  },
  data: function() {
    return {
      width: 80,
      height: 60
    };
  },
  computed: {
    translateTop: function() {
      return "translate(50 20)";
    },
    translateBottom: function() {
      return "translate(50 " + (20 + this.height) + ")";
    },
    rx: function() {
      return this.width / 2;
    },
    ry: function() {
      return (this.height / 2) * (1 / 3);
    }
  },
  methods: {
    showValue: function() {
      console.log("called showValue");
    }
  }
};
</script>