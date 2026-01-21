import { config } from '../config';
import { Strategy, StressEvent, mockStrategies, stressEvents } from '../data/strategies';

export interface IStrategyService {
    getStrategies(): Promise<Strategy[]>;
    getStrategyById(id: string): Promise<Strategy | undefined>;
    getStressEvents(): Promise<StressEvent[]>;
}

class MockStrategyService implements IStrategyService {
    async getStrategies(): Promise<Strategy[]> {
        // Simulate network delay
        await new Promise(resolve => setTimeout(resolve, 500));
        return mockStrategies;
    }

    async getStrategyById(id: string): Promise<Strategy | undefined> {
        await new Promise(resolve => setTimeout(resolve, 300));
        return mockStrategies.find(s => s.id === id);
    }

    async getStressEvents(): Promise<StressEvent[]> {
        await new Promise(resolve => setTimeout(resolve, 300));
        return stressEvents;
    }
}

class ApiStrategyService implements IStrategyService {
    private baseUrl = `${config.apiBaseUrl}/ranking`;

    async getStrategies(): Promise<Strategy[]> {
        const response = await fetch(`${this.baseUrl}/strategies`);
        if (!response.ok) {
            throw new Error(`Failed to fetch strategies: ${response.statusText}`);
        }
        return response.json();
    }

    async getStrategyById(id: string): Promise<Strategy | undefined> {
        const response = await fetch(`${this.baseUrl}/${id}`);
        if (!response.ok) {
            if (response.status === 404) return undefined;
            throw new Error(`Failed to fetch strategy ${id}: ${response.statusText}`);
        }
        return response.json();
    }

    async getStressEvents(): Promise<StressEvent[]> {
        // Assuming endpoint exists
        const response = await fetch(`${this.baseUrl}/stress-events`);
        if (!response.ok) throw new Error(`Failed to fetch stress events: ${response.statusText}`);
        return response.json();
    }
}

// Factory
export const StrategyService: IStrategyService = config.useMockData
    ? new MockStrategyService()
    : new ApiStrategyService();
