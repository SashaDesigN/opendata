const fs = require("fs");
const csv = require("csv-parser");
const log = (...a) => console.log(...a);

const results = [];
const objects = {};

const read = async (file, transform = null) => {
  return new Promise((res) => {
    let data = [];
    fs.createReadStream(file)
      .pipe(csv())
      .on("data", (row) => {
        data.push(row);
      })
      .on("end", () => {
        console.log("completed ", file);
        log("items:", data.length);

        if (transform) {
          return res(transform(data));
        }
        return res(data);
      })
      .on("error", (err) => {
        console.log("err.get ", file, " - ", err);
        return res([]);
      });
  });
};

const transformByKey = (key, obj) => {
  let res = {};
  log("transform ", key);
  for (let i = 0; i < obj.length; i++) {
    res[obj[i][key]] = obj[i];
  }

  return res;
};

const groupByKey = (
  key,
  obj,
  hidrate = null,
  hidrate_name = null,
  group_transform = null
) => {
  let res = {};
  for (let i = 0; i < obj.length; i++) {
    if (!res[obj[i][key]]) res[obj[i][key]] = [];
    res[obj[i][key]].push(obj[i]);
  }

  if (hidrate && hidrate_name) {
    log("do hidrate ", key);
    Object.keys(hidrate).map((key) => {
      if (res[key]) {
        hidrate[key][hidrate_name] = group_transform
          ? group_transform(res[key])
          : res[key];
      }
    });
    return hidrate;
  }

  return res;
};

const build = async () => {
  // get all images
  const constituents = await read("constituents.csv", (data) =>
    transformByKey("constituentid", data)
  );
  let objects = await read("objects.csv", (data) =>
    transformByKey("objectid", data)
  );
  objects = await read("objects_text_entries.csv", (data) =>
    groupByKey("objectid", data, objects, "texts")
  );
  objects = await read("objects_terms.csv", (data) =>
    groupByKey("objectid", data, objects, "terms")
  );

  objects = await read("objects_constituents.csv", (data) =>
    groupByKey("objectid", data, objects, "constituents", (con) => {
      return con
        .map((c) => {
          if (constituents[c.constituentid])
            return constituents[c.constituentid];
          else return null;
        })
        .filter((c) => c);
    })
  );

  let images = await read("published_images.csv", (data) => {
    for (let i = 0; i < data.length; i++) {
      if (objects[data[i].depictstmsobjectid])
        data[i].object = objects[data[i].depictstmsobjectid];
    }
    return data;
  });
  // const history = await read("objects_historical_data.csv");

  // get all objects
  const medias = {};
  // log("obj.res:", objects[101]);
  let expo_res = [];
  for (let i = 1; i < 30; i++) {
    expo_res.push(images[i]);
  }
  fs.writeFileSync(
    "sample_res.json",
    JSON.stringify(expo_res, null, 2),
    "utf8"
  );
};

build();
