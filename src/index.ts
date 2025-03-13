import { run } from './execution';
import { def, wrap } from './config';

export const balar = {
  def,
  wrap,
  run,
};

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<Map<Budget, string | null>> {
  return balar.run(requests, (updateBudget: BudgetUpdateRequest) => {
    if (updateBudget.amount === 0) {
      return 'budget should be greater than 0';
    }

    const currentBudget = await repository.getBudgets(request.id);
    if (updateBudget.amount < currentBudget!.amount) {
      return 'budget must not be lowered';
    }

    const success = await repository.updateBudgets(request);
    if (!success) {
      return 'budget update failed';
    }

    return true;
  });
}
