/// <reference types="vite/client" />

declare module 'react-virtualized-auto-sizer' {
    import { ComponentType, ReactNode } from 'react';

    export interface AutoSizerProps {
        children: (props: { height: number; width: number }) => ReactNode;
        className?: string;
        defaultHeight?: number;
        defaultWidth?: number;
        disableHeight?: boolean;
        disableWidth?: boolean;
        onResize?: (size: { height: number; width: number }) => void;
        style?: React.CSSProperties;
    }

    const AutoSizer: ComponentType<AutoSizerProps>;
    export default AutoSizer;
}

declare module 'react-window' {
    import { ComponentType, CSSProperties, Key } from 'react';

    export interface ListProps {
        children: ComponentType<{
            index: number;
            style: CSSProperties;
            data?: any;
        }>;
        className?: string;
        direction?: 'horizontal' | 'vertical';
        height: number;
        initialScrollOffset?: number;
        innerElementType?: string | ComponentType;
        innerTagName?: string; // deprecated
        itemCount: number;
        itemData?: any;
        itemKey?: (index: number, data: any) => Key;
        itemSize: number | ((index: number) => number);
        layout?: 'horizontal' | 'vertical';
        onItemsRendered?: (props: {
            overscanStartIndex: number;
            overscanStopIndex: number;
            visibleStartIndex: number;
            visibleStopIndex: number;
        }) => void;
        onScroll?: (props: {
            scrollDirection: 'forward' | 'backward';
            scrollOffset: number;
            scrollUpdateWasRequested: boolean;
        }) => void;
        outerElementType?: string | ComponentType;
        outerTagName?: string; // deprecated
        overscanCount?: number;
        ref?: any;
        style?: CSSProperties;
        useIsScrolling?: boolean;
        width: number;
    }

    export const FixedSizeList: ComponentType<ListProps>;
    export const VariableSizeList: ComponentType<ListProps>;
}

interface Window {
    __BACKTEST_UI_CONFIG__?: {
        backtestApiBaseUrl?: string;
    };
}
