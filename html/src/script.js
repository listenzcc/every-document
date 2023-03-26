console.log("script.js is loaded.");
console.log("d3 is loaded: ", d3);

function rawJson2Array(raw) {
    const columns = [],
        array = [];

    for (let attr in raw) {
        columns.push(attr);
    }

    for (let i in raw[columns[0]]) {
        const obj = {};
        columns.map((col) => (obj[col] = raw[col][i]));
        array.push(obj);
    }

    array.columns = columns;

    return array;
}

function updateRuntimeResult(raw, path = "[Path]", query = undefined) {
    const array = rawJson2Array(raw),
        { columns } = array;

    console.log(raw);
    console.log(array);

    document.getElementById("runtime-result").innerHTML = "";

    const container = d3.select("#runtime-result").append("div");

    container.append("div").text(path + ": " + array.length);

    container
        .append("ol")
        .selectAll("li")
        .data(array)
        .enter()
        .append("li")
        .append("div")
        .selectAll("p")
        .data((d) => columns.map((col) => col + ":\t" + d[col]))
        .enter()
        .append("p")
        .html((d) =>
            query
                ? d.replace(query, `<span style="color: red">${query}</span>`)
                : d
        );
}

function queryAllFile() {
    const path = "./query/all-file";
    d3.json(path).then((raw) => {
        updateRuntimeResult(raw, path);
    });
}

function queryFindFile(query) {
    console.log("Query Find File, " + query);
    const path = `./query/find-file?query=${query}`;
    d3.json(path).then((raw) => {
        updateRuntimeResult(raw, path, query);
    });
}

function queryFindMarkdown(query) {
    console.log("Query Find Markdown, " + query);
    const path = `./query/find-markdown?query=${query}`;
    d3.json(path).then((raw) => {
        updateRuntimeResult(raw, path, query);
    });
}
