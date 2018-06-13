USE [master]
GO

CREATE DATABASE TicketService;
GO

USE [TicketService];
GO

CREATE SCHEMA TS;
GO


CREATE TABLE [TS].[Stage] 
(
	LevelId tinyint NOT NULL,
	LevelName VARCHAR(30) NOT NULL,
	TotalTows smallint NOT NULL,
	SeatsInRow smallint NOT NULL,
	Price decimal(5,2) NOT NULL,
	CONSTRAINT PK_Stage_LevelId PRIMARY KEY CLUSTERED (LevelId)
);

GO

INSERT INTO [TS].[Stage] (LevelId, LevelName, TotalTows, SeatsInRow, Price) VALUES(1, 'Orchestra', 25, 50, 100.00);
INSERT INTO [TS].[Stage] (LevelId, LevelName, TotalTows, SeatsInRow, Price) VALUES(2, 'Main', 20, 100, 75.00);
INSERT INTO [TS].[Stage] (LevelId, LevelName, TotalTows, SeatsInRow, Price) VALUES(3, 'Balcony 1', 15, 100, 50.00);
INSERT INTO [TS].[Stage] (LevelId, LevelName, TotalTows, SeatsInRow, Price) VALUES(4, 'Balcony 2', 15, 100, 40.00);
GO

CREATE TABLE [TS].[SeatHold] 
(
	SeatHoldId int IDENTITY (1,1) NOT NULL,
	CustomerEmail VARCHAR(128) NOT NULL,
	ExpiringAt DateTime2 NOT NULL,
	Deleted Bit NOT NULL DEFAULT(0)
);
GO

CREATE NONCLUSTERED INDEX IDX_SeatHold_SeatHoldId 
	ON [TS].[SeatHold] (SeatHoldId)
	INCLUDE(CustomerEmail, ExpiringAt);
GO

CREATE NONCLUSTERED INDEX IDX_SeatHold_CustomerEmail 
	ON [TS].[SeatHold] (CustomerEmail, Deleted)
	INCLUDE (SeatHoldId, ExpiringAt);
GO

CREATE NONCLUSTERED INDEX IDX_SeatHold_Deleted 
	ON [TS].[SeatHold] (SeatHoldId, Deleted);
	
GO

CREATE TABLE [TS].[Reservation] 
(
	ReservationId INT IDENTITY (1,1) NOT NULL,
	ConfirmationId VARCHAR(30) NOT NULL,
	CustomerEmail varchar(128) NOT NULL,
	TotalPaid decimal(7,2) NOT NULL,
	ReservedTime DateTime2 NOT NULL,
	Cancelled BIT NOT NULL DEFAULT(0),
	CancelledTime DateTime2,
	CONSTRAINT PK_Reservation_ReservationId PRIMARY KEY CLUSTERED (ReservationId)
);

GO



CREATE TABLE [TS].[SeatMap] 
(
	SeatId int IDENTITY (1,1) NOT NULL,
	LevelId tinyint NOT NULL,
	RowNumber smallint NOT NULL,
	SeatNumber smallint NOT NULL,
	Status tinyint NOT NULL DEFAULT(0),
	SeatHoldId int,
	ReservationId int,
	CONSTRAINT PK_SeatMap_SeatId PRIMARY KEY CLUSTERED (SeatId),
	CONSTRAINT FK_SeatMap_Stage_LevelId FOREIGN KEY (LevelId) REFERENCES [TS].[Stage] (LevelId),
	CONSTRAINT FK_SeatMap_Reservation_ReservationId FOREIGN KEY (ReservationId) REFERENCES [TS].[Reservation] (ReservationId)
);
GO

CREATE NONCLUSTERED INDEX IDX_SeatMap_Status 
	ON [TS].[SeatMap] (Status, LevelId, RowNumber, SeatNumber) 
	INCLUDE (SeatId,  SeatHoldId, ReservationId);

GO

CREATE NONCLUSTERED INDEX IDX_SeatMap_SeatHoldId
	ON [TS].[SeatMap] ([SeatHoldId]);
GO

CREATE NONCLUSTERED INDEX IDX_SeatMap_ReservationId
	ON [TS].[SeatMap] ([ReservationId]);
GO



/*
* Procedures
*
*
*/

CREATE PROCEDURE GetFreeSeatCount 
AS 
BEGIN
	SELECT COUNT(*) AS Seats
	FROM [TS].[SeatMap]
	WHERE Status = 0;
END
GO

CREATE PROCEDURE GetFreeSeatCountForLevel @level int
AS 
BEGIN
	SELECT COUNT(*) AS Seats
	FROM [TS].[SeatMap]
	WHERE Status = 0
		AND LevelId = @level;
END
GO

/*
 * 
 * @seatCount input param - Number of seats to hold
 * @minLevel input param - Minimum seating level
 * @maxLevel input param - Maximum seating level
 * @email input param - Customer email address
 * 
 * Hold @seatCount seats if available for the customer email.
 * 
 */
CREATE PROCEDURE HoldSeats
	@seatCount int,
	@minLevel int,
	@maxLevel int,
	@email varchar(128)
AS
BEGIN
	DECLARE @seatsAvailable int;
	DECLARE @seatHoldId int;
	DECLARE @seatId int;
	
	BEGIN TRANSACTION
		BEGIN TRY
			SELECT @seatsAvailable = COUNT(*)
			FROM [TS].[SeatMap]
			WHERE 1 = 1
				AND Status = 0
				AND LevelId >= @minLevel 
				AND LevelId <= @maxLevel;
			
			IF (@seatsAvailable >= @seatCount) 
			BEGIN
				INSERT INTO [TS].[SeatHold](CustomerEmail, ExpiringAt) VALUES(@email, DATEADD(second, 300, CURRENT_TIMESTAMP));
				SELECT @seatHoldId = SCOPE_IDENTITY();
				
				WITH CT_SEATS AS
				(
					SELECT TOP (@seatCount) SeatId
					FROM [TS].[SeatMap]
					WHERE 1 = 1
						AND Status = 0
						AND LevelId >= @minLevel 
						AND LevelId <= @maxLevel
					ORDER BY SeatId, LevelId, RowNumber, SeatNumber
				)
				UPDATE [TS].[SeatMap] 
					SET status = 1,
						SeatHoldId = @seatHoldId
				FROM [TS].[SeatMap] S
				JOIN CT_SEATS CT ON (S.SeatId = CT.SeatId);
			END 
		END TRY
	BEGIN CATCH
		ROLLBACK;
		RETURN
	END CATCH
	
	COMMIT;
	
	SELECT SH.[SeatHoldId], SM.[SeatId], SM.[LevelId], SM.[RowNumber], SM.[SeatNumber]
	FROM [TS].[SeatHold] SH 
	JOIN [TS].[SeatMap] SM ON (SH.[SeatHoldId] = SM.[SeatHoldId])
	WHERE SH.[SeatHoldId] = @seatHoldId;
END 
GO

/*
*
*
*/
CREATE PROCEDURE [dbo].[ReserveSeats] 
	@seatHoldId int,
	@email varchar(128),
	@confirmationId varchar(30),
	@paid DECIMAL(7, 2)
AS
BEGIN
	DECLARE @diff int
	DECLARE @reservationId int
	
	SELECT @diff = DATEDIFF(second, CURRENT_TIMESTAMP, [ExpiringAt])
	FROM [TS].[SeatHold]
	WHERE 1 = 1
		AND [SeatHoldId] = @seatHoldId
		AND [Deleted] = 0;

	IF @diff IS NOT NULL AND @diff >= 0
	BEGIN
		BEGIN TRANSACTION;
		BEGIN TRY
			INSERT INTO [TS].[Reservation]([ConfirmationId], [CustomerEmail], [TotalPaid], [ReservedTime])
				VALUES(@confirmationId, @email, @paid, CURRENT_TIMESTAMP);

			SELECT @reservationId = SCOPE_IDENTITY();

			UPDATE [TS].[SeatMap]
			SET [Status] = 2,
				[SeatHoldId] = null,
				[ReservationId] = @reservationId
			WHERE [SeatHoldId] = @seatHoldId;

			UPDATE [TS].[SeatHold]
				SET [Deleted] = 1
			WHERE [SeatHoldId] = @seatHoldId;

			COMMIT TRANSACTION ;

			SELECT @reservationId;
			return;

		END TRY

		BEGIN CATCH
			ROLLBACK TRANSACTION;
			THROW;
		END CATCH
	END
 ELSE 
	BEGIN
	DECLARE @message varchar(128) = 'Ticket hold is not present or already expired.';
		THROW 55000, @message, 1;
	END
END

GO

CREATE PROCEDURE ReleaseHold
	@seatHoldId int
AS
BEGIN
	IF EXISTS( SELECT [SeatHoldId] FROM [TS].[SeatHold] WHERE [SeatHoldId] = @seatHoldId AND [Deleted] = 0)
	BEGIN
		UPDATE [TS].[SeatHold]
			SET [Deleted] = 1
		WHERE [SeatHoldId] = @seatHoldId;

		UPDATE [TS].[SeatMap]
			SET [Status] = 0,
				[SeatHoldId] = null
		WHERE [SeatHoldId] = @seatHoldId;
	END
END
GO
















	 

