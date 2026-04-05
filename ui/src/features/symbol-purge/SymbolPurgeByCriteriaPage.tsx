import { SymbolPurgeCandidateReview } from './components/SymbolPurgeCandidateReview';
import { SymbolPurgeCriteriaPanel } from './components/SymbolPurgeCriteriaPanel';
import { SymbolPurgeExecutionPanel } from './components/SymbolPurgeExecutionPanel';
import { useSymbolPurgeController } from './hooks/useSymbolPurgeController';

export function SymbolPurgeByCriteriaPage() {
  const controller = useSymbolPurgeController();

  return (
    <div className="grid gap-4 lg:grid-cols-[390px_1fr]">
      <SymbolPurgeCriteriaPanel controller={controller} />
      <section className="space-y-4">
        <SymbolPurgeCandidateReview controller={controller} />
        <SymbolPurgeExecutionPanel controller={controller} />
      </section>
    </div>
  );
}
