// Minimal Node-RED settings for the project.
module.exports = {
    flowFile: "flows.json",
    flowFilePretty: true,          // readable diffs in Git
    credentialSecret: false,       // no secrets in this lab (anonymous MQTT)
    uiPort: process.env.PORT || 1880,
    diagnostics: { enabled: false },
    editorTheme: { projects: { enabled: false }, tours: false },
    logging: { console: { level: "info", metrics: false, audit: false } },
    functionGlobalContext: {}
};
