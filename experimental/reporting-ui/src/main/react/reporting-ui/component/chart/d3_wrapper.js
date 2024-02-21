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
import { formatNumberWithMagnitude } from '../../util/formatting';
import './d3_wrapper.css';

export const removeGraph = (cardId) => {
    d3.select(`#${cardId}-chart`).selectAll("*").remove();
    d3.select(`#${cardId}-legend`).selectAll("*").remove();
}

const initializeGraph = (cardId, dimensions, isLegend = false) => {
    // Specify the chart’s dimensions.
    const width = dimensions.width;
    const height = dimensions.height;

    // Create the SVG container.
    const svg = d3.select(`#${cardId}`).append('svg')
        .attr("width", width)
        .attr("height", height)
        .attr("viewBox", [0, 0, width, height])
        .attr("class", isLegend ? "legend" : "chart-card");

    return svg;
}

const setUpUtcScale = (svg, data, dimensions, margins) => {
    // Create the positional scale.
    const x = d3.scaleUtc()
        .domain(d3.extent(data, d => d.variable))
        .range([margins.left, dimensions.width - margins.right]);

    // Add the horizontal axis.
    const arr = new Set(data.map(item => item.variable.toString())).size
    const ticks = Math.min(arr - 1, dimensions.width / 80)
    svg.append("g")
        .attr("transform", `translate(0,${dimensions.height - margins.bottom})`)
        .call(
        d3.axisBottom(x)
            .ticks(ticks)
            .tickSizeOuter(0)
            .tickFormat(d3.timeFormat('%m/%d/%y'))
        );

    return x;
}

const setUpLinearXScale = (svg, data, dimensions, margins) => {
    // Create the positional scales.
    const x = d3.scaleLinear()
        .domain([d3.min(data, d => d.group), d3.max(data, d => d.group)])
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

const setUpScaleBandXScale = (svg, data, dimensions, margins) => {
    // Create the positional scales.
    const x = d3.scaleBand()
        .domain(new Set(data.map(d => d.variable)))
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

const setUpLinearYScale = (svg, data, dimensions, margins, isPercent = false) => {
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
        .attr("x2", dimensions.width - margins.right - 10)
        .attr("stroke", "#E3E3E3"))

    return y;
}

const drawMultiLines = (svg, groups, groupColors) => {
    // Draw the lines.
    const line = d3.line().curve(d3.curveMonotoneX);
    svg.append("g")
        .attr("fill", "none")
        .attr("stroke-width", 1.5)
        .attr("stroke-linejoin", "round")
        .attr("stroke-linecap", "round")
      .selectAll("path")
      .data(groups.values())
      .join("path")
        .style("stroke", function(d){ return groupColors[d.z] })
        .style("mix-blend-mode", "multiply")
        .attr("d", line);
}

const drawBar = (svg, data, x, y, groupColors) => {
    const subX = d3.scaleBand()
        .domain(new Set(data.map(d => d.group)))
        .rangeRound([0, x.bandwidth()])
        .padding(0.05);

    const groupedData = d3.group(data, d => d.variable);

    svg.append("g")
        .selectAll()
        .data(groupedData)
        .join("g")
            .attr("transform", ([variable]) => `translate(${x(variable)},0)`)
        .selectAll()
        .data(([, d]) => d)
        .join("rect")
            .attr("x", d => subX(d.group))
            .attr("y", d => y(d.value))
            .attr("width", subX.bandwidth())
            .attr("height", d => y(0) - y(d.value))
            .attr("fill", d => groupColors[d.group]);
}

export const createMultiLineChart = (cardId, data, dimensions, margins, colorMap) => { 
    const svg = initializeGraph(`${cardId}-chart`, dimensions);
    const x = setUpUtcScale(svg, data, dimensions, margins);
    const y = setUpLinearYScale(svg, data, dimensions, margins);

    // Compute the points in pixel space as [x, y, z], where z is the name of the series.
    const points = data.map((d) => [x(d.variable), y(d.value), d.group]);

    // Group the points by series.
    const groups = d3.rollup(points, v => Object.assign(v, {z: v[0][2]}), d => d[2]);

    drawMultiLines(svg, groups, colorMap);
}

export const createPercentMultiLineChart = (cardId, data, dimensions, margins, colorMap) => { 
    const svg = initializeGraph(`${cardId}-chart`, dimensions);
    const x = setUpLinearXScale(svg, data, dimensions, margins)
    const y = setUpLinearYScale(svg, data, dimensions, margins, true);

    // Compute the points in pixel space as [x, y, z], where z is the name of the series.
    const points = data.map((d) => [x(d.variable), y(d.value), d.group]);

    // Group the points by series.
    const groups = d3.rollup(points, v => Object.assign(v, {z: v[0][2]}), d => d[2]);

    drawMultiLines(svg, groups, colorMap);
}

export const createPercentBarChart = (cardId, data, dimensions, margins) => {
    const svg = initializeGraph(`${cardId}-chart`, dimensions);
    const x = setUpScaleBandXScale(svg, data, dimensions, margins);
    const y = setUpLinearYScale(svg, data, dimensions, margins, true)

    drawBar(svg, data, x, y);
}

export const createBarChart = (cardId, data, dimensions, margins, colorMap) => {
    const svg = initializeGraph(`${cardId}-chart`, dimensions);
    const x = setUpScaleBandXScale(svg, data, dimensions, margins);
    const y = setUpLinearYScale(svg, data, dimensions, margins, false)

    drawBar(svg, data, x, y, colorMap);
}

export const createLegend = (cardId, dimensions, colorMap) => {
    const svg = initializeGraph(`${cardId}-legend`, dimensions, true);
    const keys = Object.keys(colorMap);

    var size = 20
    svg.selectAll("legendDots")
        .data(keys)
        .enter()
        .append("rect")
            .attr("x", 0)
            .attr("y", function(d,i){ return  i*(size+5)}) // 100 is where the first dot appears. 25 is the distance between dots
            .attr("width", size)
            .attr("height", size)
            .style("fill", function(d){ return colorMap[d]})
        
    svg.selectAll("legendLabels")
        .data(keys)
        .enter()
        .append("text")
        .attr("x", size*1.2)
        .attr("y", function(d,i){ return i*(size+5) + (size/2)}) // 100 is where the first dot appears. 25 is the distance between dots
        .style("fill", function(d){ return colorMap[d]})
        .text(function(d){ return d})
        .attr("text-anchor", "left")
        .style("alignment-baseline", "middle")
}
