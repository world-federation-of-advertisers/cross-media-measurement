// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as d3 from 'd3';
import { formatNumberWithMagnitude } from '../../../util/formatting';

const initializeGraph = (cardId, dimensions) => {
    // Specify the chart’s dimensions.
    const width = dimensions.width;
    const height = dimensions.width * 0.6; // TODO: check with UX on how we want to make this reactive

    // Create the SVG container.
    const svg = d3.select(`#${cardId}-line`).append('svg')
        .attr("width", width)
        .attr("height", height)
        .attr("viewBox", [0, 0, width, height])
        .attr("style", "max-width: 100%; height: auto; overflow: visible; font: 10px sans-serif;");
  
    return svg;
}

const setupUtcScale = (svg, data, dimensions, margins) => {
    // Create the positional scale.
    const x = d3.scaleUtc()
        .domain(d3.extent(data, d => d.date))
        .range([margins.left, dimensions.width - margins.right]);

    // Add the horizontal axis.
    svg.append("g")
        .attr("transform", `translate(0,${dimensions.height - margins.bottom})`)
        .call(
        d3.axisBottom(x)
            .ticks(dimensions.width / 80)
            .tickSizeOuter(0)
            .tickFormat(d3.timeFormat('%m/%d/%y'))
        );

    return x;
}

const setupLinearXScale = (svg, data, dimensions, margins) => {
    // Create the positional scales.
    const x = d3.scaleLinear()
        .domain([d3.min(data, d => d.x), d3.max(data, d => d.x)])
        .range([margins.left, dimensions.width - margins.right]);

    // Add the horizontal axis.
    svg.append("g")
        .attr("transform", `translate(0,${dimensions.height - margins.bottom})`)
        .call(
            d3.axisBottom(x)
                .ticks(8)
                .tickSizeOuter(0)
        );
    return x;
}

const setupScaleBandXScale = (svg, data, dimensions, margins) => {
    // Create the positional scales.
    const x = d3.scaleBand()
        .domain(data.map(x => x.cat))
        .range([margins.left, dimensions.width - margins.right])
        .padding(0.5);

    // Add the horizontal axis.
    svg.append("g")
        .attr("transform", `translate(0,${dimensions.height - margins.bottom})`)
        .call(
        d3.axisBottom(x)
        .ticks(8)
        .tickSizeOuter(0)
    );

    return x;
}

const setupLinearYScale = (svg, data, dimensions, margins, isPercent = false) => {
    const range = isPercent
        ? [0, 100]
        : [0, d3.max(data, d => d.value)]
    
    const y = d3.scaleLinear()
        .domain(range).nice()
        .range([dimensions.height - margins.bottom, margins.top]);

    // Add the vertical axis.
    const tickCount = isPercent ? 10 : dimensions.height/ 40;
    const tickFormat = (d, i) => {
        if (isPercent) {
            return i % 2 !== 0 ? '' : `${d}%`
        } else {
            return i % 2 !== 0 ? "" : formatNumberWithMagnitude(d, 1)
        }
    }
    svg.append("g")
        .attr("transform", `translate(${margins.left},0)`)
        .call(
            d3.axisLeft(y)
                .ticks(tickCount)
                .tickSize(0)
                .tickFormat((d, i) => {
                    return tickFormat(d, i)
                }))
        .call(g => g.select(".domain").remove())
        .call(g => g.selectAll(".tick line").clone()
        .attr("x2", dimensions.width)
        .attr("stroke", "#E3E3E3"))

    return y;
}

const drawMultiLines = (svg, groups, groupColors) => {
    // Draw the lines.
    const line = d3.line().curve(d3.curveMonotoneX);;
    svg.append("g")
        .attr("fill", "none")
        .attr("stroke-width", 1.5)
        .attr("stroke-linejoin", "round")
        .attr("stroke-linecap", "round")
      .selectAll("path")
      .data(groups.values())
      .join("path")
        .attr("stroke", function(d){ return groupColors[d.z] })
        .style("mix-blend-mode", "multiply")
        .attr("d", line);
}

const drawBar = (svg, data, x, y) => {
    svg.append("g")
        .attr("fill", "steelblue")
    .selectAll()
    .data(data)
    .join("rect")
        .attr("x", (d) => x(d.cat))
        .attr("y", (d) => y(d.val))
        .attr("height", (d) => y(0) - y(d.val))
        .attr("width", x.bandwidth());
}

export const createMultiLineChart = (cardId, data, dimensions, margins, colorMap) => { 
    const svg = initializeGraph(cardId, dimensions);
    const x = setupUtcScale(svg, data, dimensions, margins);
    const y = setupLinearYScale(svg, data, dimensions, margins);

    // Compute the points in pixel space as [x, y, z], where z is the name of the series.
    const points = data.map((d) => [x(d.date), y(d.value), d.pub]);

    // Group the points by series.
    const groups = d3.rollup(points, v => Object.assign(v, {z: v[0][2]}), d => d[2]);

    drawMultiLines(svg, groups, colorMap);
}

export const createPercentMultiLineChart = (cardId, data, dimensions, margins, colorMap) => { 
    const svg = initializeGraph(cardId, dimensions);
    const x = setupLinearXScale(svg, data, dimensions, margins)
    const y = setupLinearYScale(svg, data, dimensions, margins, true);

    // Compute the points in pixel space as [x, y, z], where z is the name of the series.
    const points = data.map((d) => [x(d.x), y(d.value), d.cat]);

    // Group the points by series.
    const groups = d3.rollup(points, v => Object.assign(v, {z: v[0][2]}), d => d[2]);

    drawMultiLines(svg, groups, colorMap);
}

export const createPercentBarChart = (cardId, data, dimensions, margins) => {
    const svg = initializeGraph(cardId, dimensions);
    const x = setupScaleBandXScale(svg, data, dimensions, margins);
    const y = setupLinearYScale(svg, data, dimensions, margins, true)

    drawBar(svg, data, x, y);
}
