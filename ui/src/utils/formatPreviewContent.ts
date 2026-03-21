const JSON_EXTENSIONS = new Set(['json', 'jsonl', 'ndjson']);
const JSON_CONTENT_TYPE_HINTS = [
  'application/json',
  'application/ld+json',
  'application/x-ndjson',
  'text/json'
];

const getFileExtension = (path?: string | null): string => {
  if (!path) {
    return '';
  }

  const cleanPath = path.split('?')[0] || '';
  const filename = cleanPath.split('/').pop() || cleanPath;
  const lastDot = filename.lastIndexOf('.');

  if (lastDot < 0 || lastDot === filename.length - 1) {
    return '';
  }

  return filename.slice(lastDot + 1).toLowerCase();
};

const tryFormatJson = (value: string): string | null => {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return null;
  }
};

const isLikelyJsonContent = (
  content: string,
  path?: string | null,
  contentType?: string | null
): boolean => {
  const extension = getFileExtension(path);
  if (JSON_EXTENSIONS.has(extension)) {
    return true;
  }

  const normalizedContentType = String(contentType || '').toLowerCase();
  if (JSON_CONTENT_TYPE_HINTS.some((hint) => normalizedContentType.includes(hint))) {
    return true;
  }

  const trimmed = content.trim();
  return trimmed.startsWith('{') || trimmed.startsWith('[');
};

export const formatPreviewContent = (
  content?: string | null,
  options?: {
    path?: string | null;
    contentType?: string | null;
  }
): string => {
  if (!content) {
    return '';
  }

  if (!isLikelyJsonContent(content, options?.path, options?.contentType)) {
    return content;
  }

  const fullDocument = tryFormatJson(content.trim());
  if (fullDocument) {
    return fullDocument;
  }

  let formattedLineCount = 0;
  const lines = content.split(/\r?\n/).map((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return '';
    }

    const formattedLine = tryFormatJson(trimmed);
    if (!formattedLine) {
      return line;
    }

    formattedLineCount += 1;
    return formattedLine;
  });

  return formattedLineCount > 0 ? lines.join('\n') : content;
};
