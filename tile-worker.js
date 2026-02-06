let wasmModule = null;

// Initialize WASM module when worker starts
self.onmessage = async (event) => {
    const { type, data } = event.data;
    console.log(`Worker received message of type: ${type}. Loaded WASM: ${wasmModule !== null}`);

    if (type === 'init') {
        // Load WASM module once
        const { default: init, init_panic_hook, get_image} =
            await import('./wplacearchive.js');
        await init();
        init_panic_hook();
        wasmModule = { get_image };
        self.postMessage({ type: 'ready' });
        return;
    }

    if (type === 'decompress' && wasmModule) {
        console.log(data);
        const { taskId, version, buffers } = data;
        console.log(`Worker received decompress task ${taskId} for version ${version}`);
        try {
            const uint8Array = new Uint8Array(buffers[0]);
            // compressed_bytes_to_png_blob returns a Uint8Array (PNG bytes)
            const pngBytes = wasmModule.get_image(version, uint8Array);

            // Copy the Uint8Array to create a new ArrayBuffer we can transfer
            const arrayBuffer = new Uint8Array(pngBytes).buffer;
            self.postMessage({
                type: 'decompress-result',
                taskId,
                version,
                arrayBuffer,
                error: null
            }, [arrayBuffer]); // Transfer ownership
            console.log(`Worker completed decompress task ${taskId}`);
        } catch (error) {
            self.postMessage({
                type: 'decompress-result',
                taskId,
                error: error
            });
            console.error(`Worker failed decompress task ${taskId}: ${error}`);
        }
    }

    if (type === 'decompress-diff' && wasmModule) {
        const { taskId, buffers } = data;
        console.log(`Worker received decompress-diff task ${taskId}`);
        try {
            const baseUint8Array = new Uint8Array(buffers[0]);
            const diffUint8Array = new Uint8Array(buffers[1]);
            const pngBytes = wasmModule.diff_compressed_bytes_to_png_blob(baseUint8Array, diffUint8Array);

            // Copy the Uint8Array to create a new ArrayBuffer we can transfer
            const arrayBuffer = new Uint8Array(pngBytes).buffer;
            self.postMessage({
                type: 'decompress-result',
                taskId,
                arrayBuffer,
                error: null
            }, [arrayBuffer]); // Transfer ownership
            console.log(`Worker completed decompress-diff task ${taskId}`);
        } catch (error) {
            self.postMessage({
                type: 'decompress-result',
                taskId,
                error: error.message
            });
            console.error(`Worker failed decompress-diff task ${taskId}: ${error.message}`);
        }
    }

    if (type === 'downscale' && wasmModule) {
        const { taskId, buffers } = data;
        console.log(`Worker received downscale task ${taskId}`);
        try {
            const uint8Array1 = new Uint8Array(buffers[0]);
            const uint8Array2 = new Uint8Array(buffers[1]);
            const uint8Array3 = new Uint8Array(buffers[2]);
            const uint8Array4 = new Uint8Array(buffers[3]);

            // Call the WASM function
            const pngBytes = wasmModule.compressed_4to1(
                uint8Array1,
                uint8Array2,
                uint8Array3,
                uint8Array4
            );

            // Copy the Uint8Array to create a new ArrayBuffer we can transfer
            const arrayBuffer = new Uint8Array(pngBytes).buffer;
            self.postMessage({
                type: 'downscale-result',
                taskId,
                arrayBuffer,
                error: null
            }, [arrayBuffer]); // Transfer ownership
            console.log(`Worker completed downscale task ${taskId}`);
        } catch (error) {
            self.postMessage({
                type: 'downscale-result',
                taskId,
                error: error.message
            });
            console.error(`Worker failed downscale task ${taskId}: ${error.message}`);
        }
    }

    if (type === 'downscale-diff' && wasmModule) {
        const { taskId, buffers } = data;
        console.log(`Worker received downscale-diff task ${taskId}`);
        try {
            const baseUint8Array1 = new Uint8Array(buffers[0]);
            const baseUint8Array2 = new Uint8Array(buffers[1]);
            const baseUint8Array3 = new Uint8Array(buffers[2]);
            const baseUint8Array4 = new Uint8Array(buffers[3]);
            const diffUint8Array1 = new Uint8Array(buffers[4]);
            const diffUint8Array2 = new Uint8Array(buffers[5]);
            const diffUint8Array3 = new Uint8Array(buffers[6]);
            const diffUint8Array4 = new Uint8Array(buffers[7]);

            // Call the WASM function
            const pngBytes = wasmModule.diff_compressed_4to1(
                baseUint8Array1,
                baseUint8Array2,
                baseUint8Array3,
                baseUint8Array4,
                diffUint8Array1,
                diffUint8Array2,
                diffUint8Array3,
                diffUint8Array4
            );

            // Copy the Uint8Array to create a new ArrayBuffer we can transfer
            const arrayBuffer = new Uint8Array(pngBytes).buffer;
            self.postMessage({
                type: 'downscale-result',
                taskId,
                arrayBuffer,
                error: null
            }, [arrayBuffer]); // Transfer ownership
            console.log(`Worker completed downscale-diff task ${taskId}`);
        } catch (error) {
            self.postMessage({
                type: 'downscale-result',
                taskId,
                error: error.message
            });
            console.error(`Worker failed downscale-diff task ${taskId}: ${error.message}`);
        }
    }

};