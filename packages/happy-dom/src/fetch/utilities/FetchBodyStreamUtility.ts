import { Buffer } from 'buffer';
import { ReadableStream } from 'stream/web';
import * as PropertySymbol from '../../PropertySymbol.js';
import DOMException from '../../exception/DOMException.js';
import DOMExceptionNameEnum from '../../exception/DOMExceptionNameEnum.js';
import type BrowserWindow from '../../window/BrowserWindow.js';
import WindowBrowserContext from '../../window/WindowBrowserContext.js';
import type AbortSignal from '../AbortSignal.js';

/**
 * Shared, abortable body stream consumption.
 */
export default class FetchBodyStreamUtility {
	/**
	 * Reads a body, interrupting pending reads when its frame is aborted.
	 *
	 * @param window Window.
	 * @param requestOrResponse Request or response.
	 * @param requestOrResponse.body Body stream.
	 * @param requestOrResponse.signal Request signal.
	 * @param onChunk Optional consumer for multipart parsing.
	 * @returns Body buffer.
	 */
	public static async consumeBodyStream(
		window: BrowserWindow,
		requestOrResponse: {
			body: ReadableStream | null;
			[PropertySymbol.buffer]?: Buffer | null;
			[PropertySymbol.bodyBuffer]?: Buffer | null;
			[PropertySymbol.aborted]: boolean;
			[PropertySymbol.error]: Error | null;
			signal?: AbortSignal;
		},
		onChunk?: (chunk: Uint8Array) => void
	): Promise<Buffer> {
		const buffer =
			requestOrResponse[PropertySymbol.buffer] ?? requestOrResponse[PropertySymbol.bodyBuffer];
		if (buffer) {
			onChunk?.(buffer);
			return buffer;
		}
		const body = requestOrResponse.body;
		if (body === null || !(body instanceof ReadableStream)) {
			return Buffer.alloc(0);
		}
		if (requestOrResponse[PropertySymbol.error]) {
			throw requestOrResponse[PropertySymbol.error];
		}

		const abortError = new window.DOMException(
			'Failed to read response body: The stream was aborted.',
			DOMExceptionNameEnum.abortError
		);
		const asyncTaskManager = new WindowBrowserContext(window).getAsyncTaskManager();
		// A completed network stream can still contain unread, queued chunks after close.
		const completed = (<any>body)[PropertySymbol.nodeStream]?.readableEnded === true;
		if (
			requestOrResponse[PropertySymbol.aborted] ||
			((!asyncTaskManager || window.closed) && !completed)
		) {
			throw abortError;
		}

		const reader = body.getReader();
		const chunks = [];
		let bytes = 0;
		let taskID: number | undefined;
		try {
			if (!completed) {
				taskID = asyncTaskManager!.startTask(() => {
					requestOrResponse[PropertySymbol.aborted] = true;
					// Cancelling the reader settles read() even if the source's cancel() never resolves.
					void reader.cancel(abortError).catch(() => {});
					requestOrResponse.signal?.[PropertySymbol.abort]();
				});
			}
			while (true) {
				const result = await reader.read();
				if (requestOrResponse[PropertySymbol.error]) {
					throw requestOrResponse[PropertySymbol.error];
				}
				if (requestOrResponse[PropertySymbol.aborted]) {
					throw abortError;
				}
				if (result.done) {
					break;
				}
				bytes += result.value.length;
				chunks.push(result.value);
				onChunk?.(result.value);
			}
		} catch (error) {
			if (error instanceof DOMException) {
				throw error;
			}
			throw new window.DOMException(
				`Failed to read response body. Error: ${(<Error>error).message}.`,
				DOMExceptionNameEnum.encodingError
			);
		} finally {
			reader.releaseLock();
			if (taskID !== undefined) {
				asyncTaskManager!.endTask(taskID);
			}
		}
		try {
			return typeof chunks[0] === 'string'
				? Buffer.from(chunks.join(''))
				: Buffer.concat(chunks, bytes);
		} catch (error) {
			throw new window.DOMException(
				`Could not create Buffer from response body. Error: ${(<Error>error).message}.`,
				DOMExceptionNameEnum.invalidStateError
			);
		}
	}
}
