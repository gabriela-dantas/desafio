from typing import List
from collections import defaultdict

from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import QuotaViewModel
from common.repositories.md_cota.quota_owner import QuotaOwnerRepository
from common.exceptions import EntityNotFound


class QuotaViewRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaViewModel)

    def get_data_for_bpm(self, quota_codes: List[str]) -> List[dict]:
        self._logger.debug("Buscando cotas na View.")
        quotas_with_owners = []
        owners_by_quota = defaultdict(list)

        query = self._session.query(
            QuotaViewModel.quota_id,
            QuotaViewModel.quota_code,
            QuotaViewModel.quota_number,
            QuotaViewModel.version_id,
            QuotaViewModel.group_deadline,
            QuotaViewModel.cancel_date,
            QuotaViewModel.acquisition_date,
            QuotaViewModel.contract_number,
            QuotaViewModel.per_mutual_fund_paid,
            QuotaViewModel.administrator_fee,
            QuotaViewModel.asset_value,
            QuotaViewModel.amnt_to_pay,
            QuotaViewModel.fund_reservation_fee,
            QuotaViewModel.external_reference,
            QuotaViewModel.group_code,
            QuotaViewModel.quota_origin_code,
            QuotaViewModel.administrator_code,
        ).filter(QuotaViewModel.quota_code.in_(quota_codes))

        self._logger.debug(
            f"Será executada query para busca de cotas na View:\n {self._get_raw_query(query)}"
        )

        all_quotas: List[QuotaViewModel] = query.all()
        self._logger.debug(f"Obtidas {len(all_quotas)} cotas.")

        target_ids = list(map(lambda target_quota: target_quota.quota_id, all_quotas))
        owners = QuotaOwnerRepository().get_for_many_quotas(target_ids)
        self._logger.debug(f"Obtidos {len(owners)} owners para as cotas recuperadas.")

        for owner in owners:
            owners_by_quota[owner.quota_id].append(
                {
                    "person_code": owner.person_code,
                    "titular": owner.main_owner,
                }
            )

        del owners

        for quota in all_quotas:
            if not owners_by_quota[quota.quota_id]:
                self._logger.critical(
                    f"quota_code {quota.quota_code}, "
                    f"a ser enviada ao BPM, não possui owner!"
                )
                continue

            if quota.version_id == "NA":
                version_id = None
            else:
                version_id = quota.version_id

            quotas_with_owners.append(
                {
                    "quota_number": quota.quota_number,
                    "version_id": version_id,
                    "group_deadline": quota.group_deadline,
                    "cancel_date": quota.cancel_date,
                    "acquisiton_date": quota.acquisition_date,
                    "contract_number": quota.contract_number,
                    "per_mutual_fund_paid": quota.per_mutual_fund_paid,
                    "admnistrator_fee": quota.administrator_fee,
                    "asset_value": quota.asset_value,
                    "amnt_to_pay": quota.amnt_to_pay,
                    "fund_reservation_fee": quota.fund_reservation_fee,
                    "external_reference": quota.external_reference,
                    "quota_code": quota.quota_code,
                    "group_code": quota.group_code,
                    "origin": quota.quota_origin_code,
                    "administrator_code": quota.administrator_code,
                    "owners": owners_by_quota[quota.quota_id],
                }
            )

        retrieved_quotas = list(
            map(lambda target_quota: target_quota.quota_code, all_quotas)
        )
        not_found_quotas = set(quota_codes).difference(set(retrieved_quotas))
        if not_found_quotas:
            self._logger.critical(
                "Quota Code de cotas a serem enviadas para o BPM "
                f"que não foram encontradas: {not_found_quotas}"
            )
        self._logger.debug(
            f"Obtidas {len(quotas_with_owners)} cotas com seus respectivos owners."
        )
        return quotas_with_owners

    def get_quota_code_by_contract(
        self, contract_number: str, administrator_code: str
    ) -> dict:
        self._logger.debug("Buscando cota na View pela ADM e contrato...")

        query = self._session.query(
            QuotaViewModel.quota_id, QuotaViewModel.quota_code
        ).filter(
            (QuotaViewModel.contract_number == contract_number)
            & (QuotaViewModel.administrator_code == administrator_code)
        )

        self._logger.debug(
            f"Será executada query para busca da cota na View:\n {self._get_raw_query(query)}"
        )

        quota: QuotaViewModel = query.first()
        if quota is None:
            raise EntityNotFound(
                f"Cota com contrato {contract_number} e "
                f"ADM {administrator_code} não encontrada."
            )

        self._logger.debug(f"Obtido quota_code: {quota.quota_code}")
        return {"quota_id": quota.quota_id, "quota_code": quota.quota_code}
