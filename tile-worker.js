let wasmModule = null;

// Initialize WASM module when worker starts
self.onmessage = async (event) => {
    const { type, data } = event.data;
    console.log(`Worker received message of type: ${type}`);

    if (type === 'init') {
        // Load WASM module once
        const { default: init, compressed_bytes_to_png_blob, init_panic_hook, compressed_4to1 } =
            await import('./wplace_archive_world_map.js');
        await init();
        init_panic_hook();
        wasmModule = { compressed_bytes_to_png_blob, compressed_4to1 };
        self.postMessage({ type: 'ready' });
        return;
    }

    if (type === 'decompress' && wasmModule) {
        const { taskId, buffers } = data;
        console.log(`Worker received decompress task ${taskId}`);
        try {
            const uint8Array = new Uint8Array(buffers[0]);
            // compressed_bytes_to_png_blob returns a Uint8Array (PNG bytes)
            const pngBytes = wasmModule.compressed_bytes_to_png_blob(uint8Array);

            // Copy the Uint8Array to create a new ArrayBuffer we can transfer
            const arrayBuffer = new Uint8Array(pngBytes).buffer;
            self.postMessage({
                type: 'decompress-result',
                taskId,
                arrayBuffer,
                error: null
            }, [arrayBuffer]); // Transfer ownership
            console.log(`Worker completed decompress task ${taskId}`);
        } catch (error) {
            self.postMessage({
                type: 'decompress-result',
                taskId,
                error: error.message
            });
            console.error(`Worker failed decompress task ${taskId}: ${error.message}`);
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

};