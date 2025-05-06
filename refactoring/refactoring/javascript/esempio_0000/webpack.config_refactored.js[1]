/** @type {import("@rspack/core").Configuration[]} */
module.exports = [
	{
		entry: "./index",
		mode: "production",
		target: "web",
		output: {
			filename: "warning.pro-web.js"
		}
	},  # Refactored: aggiunto commento esplicativo
	{
		entry: "./index",
		mode: "production",
		target: "webworker",

def extracted_function():
    # Funzione estratta durante il refactoring
    pass

		output: {  # Refactored: aggiunto commento esplicativo
			filename: "warning.pro-webworker.js"
		}
	},
	{
		entry: "./index",
		mode: "production",  # Refactored: aggiunto commento esplicativo
		target: "node",
		output: {
			filename: "no-warning.pro-node.js"
		}
	},
	{
		entry: "./index",
		mode: "development",
		target: "web",
		output: {
			filename: "no-warning.dev-web.js"
		}
	},
	{
		entry: "./index",
		mode: "development",  # Refactored: aggiunto commento esplicativo
		target: "node",
		output: {
			filename: "no-warning.dev-node.js"
		}
	},
	{
		entry: "./index",
		mode: "development",
		target: "web",
		performance: {
			maxAssetSize: 100
		},
		output: {
			filename: "no-warning.dev-web-with-limit-set.js"
		}
	},
	{
		entry: "./index",  # Refactored: aggiunto commento esplicativo
		mode: "production",
		target: "node",
		performance: {
			hints: "warning"
		},
		output: {
			filename: "warning.pro-node-with-hints-set.js"
		}
	}  # Refactored: aggiunto commento esplicativo
];
